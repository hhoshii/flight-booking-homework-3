package tests

import (
	"context"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// This test stops the current Redis master. Enable only for a disposable test stack.
func TestSentinelFailover(t *testing.T) {
	if os.Getenv("TEST_REDIS_FAILOVER") != "1" {
		t.Skip("set TEST_REDIS_FAILOVER=1 and TEST_REDIS_SENTINELS on a disposable stack")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	addresses := strings.Split(os.Getenv("TEST_REDIS_SENTINELS"), ",")
	sentinel := redis.NewSentinelClient(&redis.Options{Addr: addresses[0]})
	defer sentinel.Close()
	before, err := sentinel.GetMasterAddrByName(ctx, "flight-cache").Result()
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "flight-cache", SentinelAddrs: addresses, DB: 3, DialTimeout: 300 * time.Millisecond, ReadTimeout: 300 * time.Millisecond, WriteTimeout: 300 * time.Millisecond, MaxRetries: 1})
	defer client.Close()
	if err = client.Set(ctx, "sentinel-probe", "before", 30*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if n, e := client.Wait(ctx, 1, 5*time.Second).Result(); e != nil || n != 1 {
		t.Fatalf("replication not ready: replicas=%d error=%v", n, e)
	}
	oldMaster := redis.NewClient(&redis.Options{Addr: net.JoinHostPort(before[0], before[1]), ReadTimeout: time.Second})
	_ = oldMaster.Shutdown(ctx).Err()
	oldMaster.Close()
	for ctx.Err() == nil {
		after, e := sentinel.GetMasterAddrByName(ctx, "flight-cache").Result()
		if e == nil && net.JoinHostPort(after[0], after[1]) != net.JoinHostPort(before[0], before[1]) {
			value, e := client.Get(ctx, "sentinel-probe").Result()
			if e == nil && value == "before" {
				if e = client.Set(ctx, "sentinel-probe", "after", 30*time.Second).Err(); e == nil {
					t.Logf("master %v -> %v; failover client reads and writes", before, after)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("Sentinel did not recover read/write access", ctx.Err())
}
