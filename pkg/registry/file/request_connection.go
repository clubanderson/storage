package file

import (
	"context"
	"errors"
	"fmt"
	"time"

	"zombiezen.com/go/sqlite"
)

const connectionAttemptWindow = 250 * time.Millisecond

// Never reserve an idle connection while waiting for a key. Release the key too
// if a connection is unavailable, so an existing connection owner can finish.
func (s *StorageImpl) lockedConnection(ctx context.Context, key string, write bool) (*sqlite.Conn, error) {
	lock, unlock := s.locks.RLock, s.locks.RUnlock
	if write {
		lock, unlock = s.locks.Lock, s.locks.Unlock
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("acquire key connection: %w", err)
		}
		if err := lock(ctx, key); err != nil {
			return nil, fmt.Errorf("acquire key lock: %w", err)
		}
		attempt, cancel := context.WithTimeout(ctx, connectionAttemptWindow)
		conn, err := s.pool.Take(attempt)
		if err == nil {
			if err := ctx.Err(); err != nil {
				s.pool.Put(conn)
				unlock(key)
				cancel()
				return nil, fmt.Errorf("acquire key connection: %w", err)
			}
			if write {
				// A begun legacy commit retains its original non-cancellable SQL
				// lifetime; request cancellation still controls acquisition.
				conn.SetInterrupt(nil)
			} else {
				conn.SetInterrupt(ctx.Done())
			}
			cancel()
			return conn, nil
		}
		unlock(key)
		cancel()
		if ctx.Err() != nil {
			return nil, fmt.Errorf("acquire key connection: %w", ctx.Err())
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("acquire key connection: %w", err)
		}
		// Do not reacquire the key while the pool is exhausted. Its current
		// owner may need that key before it can return its connection.
		available, err := s.pool.Take(ctx)
		if err != nil {
			return nil, fmt.Errorf("wait for available connection: %w", err)
		}
		s.pool.Put(available)
	}
}
