package flight

import (
	"context"
	"log/slog"
	"sync"
	"time"

	pb "github.com/hhoshii/flight-booking-homework-3/gen/flight/v1"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const CacheTTL = 5 * time.Minute

type Cache struct {
	// Readers hold RLock across SELECT + cache fill; writers hold Lock across
	// commit + invalidation. This prevents stale fills in our single Flight instance.
	mu          sync.RWMutex
	client      redis.UniversalClient
	bypassUntil time.Time
}

func NewCache(client redis.UniversalClient) *Cache { return &Cache{client: client} }

func searchKey(origin, destination, date string) string {
	return "search:" + origin + ":" + destination + ":" + date
}

// get and put require at least mu.RLock.
func (c *Cache) get(ctx context.Context, key string, dst proto.Message) bool {
	if c.client == nil || time.Now().Before(c.bypassUntil) {
		return false
	}
	data, err := c.client.Get(ctx, key).Bytes()
	if err == nil {
		err = protojson.Unmarshal(data, dst)
	}
	if err == nil {
		slog.Info("cache hit", "key", key)
		return true
	}
	if err != redis.Nil {
		slog.Warn("cache read failed; using database", "key", key, "error", err)
	}
	slog.Info("cache miss", "key", key)
	return false
}

func (c *Cache) put(ctx context.Context, key string, value proto.Message) {
	if c.client == nil || time.Now().Before(c.bypassUntil) {
		return
	}
	data, err := protojson.Marshal(value)
	if err == nil {
		err = c.client.Set(ctx, key, data, CacheTTL).Err()
	}
	if err != nil {
		slog.Warn("cache write failed", "key", key, "error", err)
	}
}

// invalidate is called with mu.Lock even if the originating RPC was canceled.
func (c *Cache) invalidate(f *pb.Flight) {
	if c.client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := c.client.Del(ctx, "flight:"+f.Id, searchKey(f.Origin, f.Destination, f.DepartureTime.AsTime().UTC().Format("2006-01-02")), searchKey(f.Origin, f.Destination, "all")).Err()
	if err != nil {
		// Old keys may survive an outage. Never read them again before their TTL.
		c.bypassUntil = time.Now().Add(CacheTTL)
		slog.Warn("cache invalidation failed; bypassing cache for TTL", "error", err)
	}
}
