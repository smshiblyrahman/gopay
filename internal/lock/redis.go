package lock

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/smshiblyrahman/gopay/internal/domain"
)

type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (string, error)
	Release(ctx context.Context, key string, lockVal string) error
	GetStatus(ctx context.Context, paymentID string) (string, error)
	SetStatus(ctx context.Context, paymentID string, status string, ttl time.Duration) error
	Ping(ctx context.Context) error
	Close() error
}

type RedisLocker struct {
	client *redis.Client
}

func NewRedisLocker(addr string, password string, db int) *RedisLocker {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	return &RedisLocker{client: rdb}
}

func (l *RedisLocker) Ping(ctx context.Context) error {
	return l.client.Ping(ctx).Err()
}

func (l *RedisLocker) Close() error {
	return l.client.Close()
}

// Acquire tries to set lock using SET NX PX. Retries with backoff if needed up to max timeout.
func (l *RedisLocker) Acquire(ctx context.Context, key string, ttl time.Duration) (string, error) {
	val := uuid.New().String()
	lockKey := fmt.Sprintf("lock:%s", key)

	deadline := time.Now().Add(5 * time.Second)
	for {
		ok, err := l.client.SetNX(ctx, lockKey, val, ttl).Result()
		if err != nil {
			return "", fmt.Errorf("redis setnx error: %w", err)
		}
		if ok {
			return val, nil
		}

		if time.Now().After(deadline) {
			return "", domain.ErrLockAcquisitionFailed
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Release releases the distributed lock atomically via Lua script
func (l *RedisLocker) Release(ctx context.Context, key string, lockVal string) error {
	lockKey := fmt.Sprintf("lock:%s", key)
	luaScript := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	_, err := l.client.Eval(ctx, luaScript, []string{lockKey}, lockVal).Result()
	return err
}

func (l *RedisLocker) GetStatus(ctx context.Context, paymentID string) (string, error) {
	cacheKey := fmt.Sprintf("payment:%s:status", paymentID)
	val, err := l.client.Get(ctx, cacheKey).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

func (l *RedisLocker) SetStatus(ctx context.Context, paymentID string, status string, ttl time.Duration) error {
	cacheKey := fmt.Sprintf("payment:%s:status", paymentID)
	return l.client.Set(ctx, cacheKey, status, ttl).Err()
}
