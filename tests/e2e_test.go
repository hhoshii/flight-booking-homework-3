package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hhoshii/flight-booking-homework-3/db"
	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/hhoshii/flight-booking-homework-3/internal/booking"
	"github.com/hhoshii/flight-booking-homework-3/internal/flight"
	"github.com/hhoshii/flight-booking-homework-3/internal/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func testDB(t *testing.T, env, service string) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(env)
	if dsn == "" {
		t.Skip("set database URLs to run integration tests")
	}
	ctx := context.Background()
	admin, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := "test_" + uuid.NewString()[:8]
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+schema); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { pool.Close(); admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE"); admin.Close() })
	if e = db.Migrate(ctx, pool, service); e != nil {
		t.Fatal(e)
	}
	return pool
}

func TestBookingEndToEnd(t *testing.T) {
	ctx := context.Background()
	fdb := testDB(t, "TEST_FLIGHT_DATABASE_URL", "flight")
	bdb := testDB(t, "TEST_BOOKING_DATABASE_URL", "booking")
	var rdb redis.UniversalClient
	if sentinels := os.Getenv("TEST_REDIS_SENTINELS"); sentinels != "" {
		rdb = redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "flight-cache", SentinelAddrs: strings.Split(sentinels, ","), DB: 2})
	} else {
		rdb = redis.NewClient(&redis.Options{Addr: os.Getenv("TEST_REDIS_ADDR"), DB: 2})
	}
	defer rdb.Close()
	if e := rdb.FlushDB(ctx).Err(); e != nil {
		t.Fatal(e)
	}
	listener := bufconn.Listen(1 << 20)
	var failRelease atomic.Bool
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(rpc.ServerAuth("key"),
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			if failRelease.Load() && info.FullMethod == pb.FlightService_ReleaseReservation_FullMethodName {
				return nil, status.Error(codes.Unavailable, "injected Flight outage")
			}
			return next(ctx, req)
		}))
	pb.RegisterFlightServiceServer(gs, flight.New(fdb, flight.NewCache(rdb)))
	go gs.Serve(listener)
	defer gs.Stop()
	breaker := rpc.NewBreaker(2, 30*time.Millisecond, time.Second)
	cc, e := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithDisableRetry(), grpc.WithChainUnaryInterceptor(breaker.Unary, rpc.Retry(100*time.Millisecond), rpc.ClientAuth("key")))
	if e != nil {
		t.Fatal(e)
	}
	defer cc.Close()
	client := pb.NewFlightServiceClient(cc)
	httpServer := httptest.NewServer(booking.New(bdb, client).Handler())
	defer httpServer.Close()
	request := func(method, path, body string, want int) []byte {
		t.Helper()
		req, e := http.NewRequest(method, httpServer.URL+path, bytes.NewBufferString(body))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer res.Body.Close()
		data, e := io.ReadAll(res.Body)
		if e != nil {
			t.Fatal(e)
		}
		if res.StatusCode != want {
			t.Fatalf("%s %s: status=%d want=%d body=%s", method, path, res.StatusCode, want, data)
		}
		return data
	}
	user := uuid.NewString()
	id := "11111111-1111-4111-8111-111111111111"
	body := `{"user_id":"` + user + `","flight_id":"` + id + `","passenger_name":"Ivan Ivanov","passenger_email":"ivan@example.com","seat_count":2}`
	request("GET", "/health", "", 200)
	var flights []map[string]any
	if e = json.Unmarshal(request("GET", "/flights?origin=SVO&destination=LED&date=2026-12-01", "", 200), &flights); e != nil {
		t.Fatal(e)
	}
	if len(flights) != 1 {
		t.Fatal(flights)
	}
	var made struct {
		ID         string `json:"id"`
		TotalPrice int64  `json:"total_price"`
		Status     string `json:"status"`
	}
	if e = json.Unmarshal(request("POST", "/bookings", body, 201), &made); e != nil {
		t.Fatal(e)
	}
	if made.ID == "" || made.TotalPrice != 1000000 || made.Status != "CONFIRMED" {
		t.Fatal(made)
	}
	var seats int
	if e = fdb.QueryRow(ctx, "SELECT available_seats FROM flights WHERE id=$1", id).Scan(&seats); e != nil || seats != 98 {
		t.Fatalf("seats=%d err=%v", seats, e)
	}
	_, e = client.UpdateFlight(ctx, &pb.UpdateFlightRequest{FlightId: id, Price: 900000, Status: pb.FlightStatus_SCHEDULED})
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(request("GET", "/bookings/"+made.ID, "", 200), &made); e != nil {
		t.Fatal(e)
	}
	if made.TotalPrice != 1000000 {
		t.Fatal("historical price changed", made)
	}
	var list []json.RawMessage
	if e = json.Unmarshal(request("GET", "/bookings?user_id="+user, "", 200), &list); e != nil || len(list) != 1 {
		t.Fatalf("%s %v", list, e)
	}
	request("POST", "/bookings/"+made.ID+"/cancel", "", 200)
	request("POST", "/bookings/"+made.ID+"/cancel", "", 409)
	if e = fdb.QueryRow(ctx, "SELECT available_seats FROM flights WHERE id=$1", id).Scan(&seats); e != nil || seats != 100 {
		t.Fatalf("cancel: seats=%d err=%v", seats, e)
	}

	t.Run("failed reserve does not create booking", func(t *testing.T) {
		tooMany := bytes.ReplaceAll([]byte(body), []byte(`"seat_count":2`), []byte(`"seat_count":101`))
		request("POST", "/bookings", string(tooMany), 409)
		var count int
		if e = bdb.QueryRow(ctx, "SELECT count(*) FROM bookings").Scan(&count); e != nil || count != 1 {
			t.Fatalf("count=%d err=%v", count, e)
		}
	})
	t.Run("insert failure compensates reservation", func(t *testing.T) {
		// A temporary database constraint forces a failure AFTER a real gRPC reserve.
		if _, e = bdb.Exec(ctx, "ALTER TABLE bookings ADD CONSTRAINT reject_new CHECK (seat_count < 3) NOT VALID"); e != nil {
			t.Fatal(e)
		}
		defer bdb.Exec(ctx, "ALTER TABLE bookings DROP CONSTRAINT reject_new")
		bad := bytes.ReplaceAll([]byte(body), []byte(`"seat_count":2`), []byte(`"seat_count":3`))
		request("POST", "/bookings", string(bad), 500)
		if e = fdb.QueryRow(ctx, "SELECT available_seats FROM flights WHERE id=$1", id).Scan(&seats); e != nil || seats != 100 {
			t.Fatalf("compensation: seats=%d err=%v", seats, e)
		}
		var active int
		if e = fdb.QueryRow(ctx, "SELECT count(*) FROM seat_reservations WHERE status='ACTIVE'").Scan(&active); e != nil || active != 0 {
			t.Fatalf("active=%d err=%v", active, e)
		}
	})
	t.Run("cancel SQL failure keeps reservation active", func(t *testing.T) {
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(request("POST", "/bookings", body, 201), &created); err != nil {
			t.Fatal(err)
		}
		if _, err := bdb.Exec(ctx, "ALTER TABLE bookings ADD CONSTRAINT reject_cancel CHECK (status <> 'CANCELLED') NOT VALID"); err != nil {
			t.Fatal(err)
		}
		defer bdb.Exec(ctx, "ALTER TABLE bookings DROP CONSTRAINT IF EXISTS reject_cancel")
		request("POST", "/bookings/"+created.ID+"/cancel", "", 500)
		var bookingStatus, reservationStatus string
		if err := bdb.QueryRow(ctx, "SELECT status FROM bookings WHERE id=$1", created.ID).Scan(&bookingStatus); err != nil {
			t.Fatal(err)
		}
		if err := fdb.QueryRow(ctx, "SELECT status FROM seat_reservations WHERE booking_id=$1", created.ID).Scan(&reservationStatus); err != nil {
			t.Fatal(err)
		}
		if err := fdb.QueryRow(ctx, "SELECT available_seats FROM flights WHERE id=$1", id).Scan(&seats); err != nil {
			t.Fatal(err)
		}
		if bookingStatus != "CONFIRMED" || reservationStatus != "ACTIVE" || seats != 98 {
			t.Fatalf("partial cancellation: booking=%s reservation=%s seats=%d", bookingStatus, reservationStatus, seats)
		}
		if _, err := bdb.Exec(ctx, "ALTER TABLE bookings DROP CONSTRAINT reject_cancel"); err != nil {
			t.Fatal(err)
		}
		request("POST", "/bookings/"+created.ID+"/cancel", "", 200)
	})
	t.Run("failed release rolls back cancellation", func(t *testing.T) {
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(request("POST", "/bookings", body, 201), &created); err != nil {
			t.Fatal(err)
		}
		failRelease.Store(true)
		defer failRelease.Store(false)
		request("POST", "/bookings/"+created.ID+"/cancel", "", 503)
		var bookingStatus, reservationStatus string
		if err := bdb.QueryRow(ctx, "SELECT status FROM bookings WHERE id=$1", created.ID).Scan(&bookingStatus); err != nil {
			t.Fatal(err)
		}
		if err := fdb.QueryRow(ctx, "SELECT status FROM seat_reservations WHERE booking_id=$1", created.ID).Scan(&reservationStatus); err != nil {
			t.Fatal(err)
		}
		if bookingStatus != "CONFIRMED" || reservationStatus != "ACTIVE" {
			t.Fatalf("booking=%s reservation=%s", bookingStatus, reservationStatus)
		}
		failRelease.Store(false)
		request("POST", "/bookings/"+created.ID+"/cancel", "", 200)
	})
	t.Run("validation and not found", func(t *testing.T) {
		request("POST", "/bookings", `{}`, 400)
		request("POST", "/bookings", body+`{}`, 400)
		request("GET", "/flights?origin=SVO&destination=LED&date=bad", "", 400)
		request("GET", "/flights/"+uuid.NewString(), "", 404)
		request("GET", "/bookings/"+uuid.NewString(), "", 404)
		request("GET", "/bookings?user_id=bad", "", 400)
	})
	t.Run("open circuit returns immediate 503", func(t *testing.T) {
		gs.Stop()
		request("GET", "/flights/"+id, "", 503)
		request("GET", "/flights/"+id, "", 503)
		start := time.Now()
		request("GET", "/flights/"+id, "", 503)
		if time.Since(start) > 50*time.Millisecond {
			t.Fatal("open circuit waited for network")
		}
	})
}
