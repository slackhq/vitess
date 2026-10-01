/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package consultopo

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"time"

	"github.com/hashicorp/consul/api"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// consulLockDescriptor implements topo.LockDescriptor.
type consulLockDescriptor struct {
	s        *Server
	lockPath string
	lost     <-chan struct{}
}

// Lock is part of the topo.Conn interface.
func (s *Server) Lock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents, s.lockTTL)
}

// LockWithTTL is part of the topo.Conn interface.
func (s *Server) LockWithTTL(ctx context.Context, dirPath, contents string, ttl time.Duration) (topo.LockDescriptor, error) {
	// We list the directory first to make sure it exists.
	if _, err := s.ListDir(ctx, dirPath, false /*full*/); err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	return s.lock(ctx, dirPath, contents, ttl.String())
}

// LockName is part of the topo.Conn interface.
func (s *Server) LockName(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	return s.lock(ctx, dirPath, contents, topo.NamedLockTTL.String())
}

// TryLock is part of the topo.Conn interface.
func (s *Server) TryLock(ctx context.Context, dirPath, contents string) (topo.LockDescriptor, error) {
	// We list all the entries under dirPath
	entries, err := s.ListDir(ctx, dirPath, true)
	if err != nil {
		// We need to return the right error codes, like
		// topo.ErrNoNode and topo.ErrInterrupted, and the
		// easiest way to do this is to return convertError(err).
		// It may lose some of the context, if this is an issue,
		// maybe logging the error would work here.
		return nil, convertError(err, dirPath)
	}

	// If there is a file 'lock' in it then someone else already has the lock,
	// unless it is a stale orphan (see canAcquireLockFile). Throw error in this case.
	for _, e := range entries {
		if e.Name == locksFilename && e.Type == topo.TypeFile && e.Ephemeral {
			ok, err := s.canAcquireLockFile(ctx, dirPath)
			if err != nil {
				return nil, convertError(err, dirPath)
			}
			if !ok {
				return nil, topo.NewError(topo.NodeExists, fmt.Sprintf("lock already exists at path %s", dirPath))
			}
			break
		}
	}

	// everything is good let's acquire the lock.
	//
	// The checks above and the acquire below are not atomic. If another client
	// acquires the lock in between (e.g. several VTOrcs racing for the same
	// stale orphan), s.lock() does not fail fast: api.Lock.Lock() waits for the
	// lock to be released, so TryLock can block until the winner unlocks or ctx
	// expires (topo.LockTimeout, 45s by default). Mutual exclusion still holds;
	// only the non-blocking behavior is lost in that window.
	return s.lock(ctx, dirPath, contents, s.lockTTL)
}

// canAcquireLockFile returns true if TryLock may acquire the lock file under
// dirPath: it no longer exists, or it is orphaned (has no consul session, e.g.
// Destroy failed after Unlock) and was locked at least orphanLockMinAge ago.
// Younger session-less lock files are treated as held: their holder may have
// just lost its session (e.g. during a consul leader election) and still be
// running, such as an in-flight reparent that has not checked its lock yet.
func (s *Server) canAcquireLockFile(ctx context.Context, dirPath string) (bool, error) {
	lockPath := path.Join(s.root, dirPath, locksFilename)
	pair, _, err := s.kv.Get(lockPath, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return false, err
	}
	if pair == nil {
		return true, nil
	}
	if pair.Session != "" {
		return false, nil
	}

	var contents struct {
		Time string
	}
	if err := json.Unmarshal(pair.Value, &contents); err != nil {
		log.Warningf("Not acquiring orphaned lock file at path %s: cannot parse its contents: %v", dirPath, err)
		return false, nil
	}
	lockedAt, err := time.Parse(time.RFC3339, contents.Time)
	if err != nil {
		log.Warningf("Not acquiring orphaned lock file at path %s: cannot parse its lock time %q: %v", dirPath, contents.Time, err)
		return false, nil
	}
	age := time.Since(lockedAt)
	if age < s.orphanLockMinAge {
		return false, nil
	}

	log.Infof("Found orphaned lock file at path %s without a session, locked %v ago, acquiring it", dirPath, age.Round(time.Second))
	return true, nil
}

// Lock is part of the topo.Conn interface.
func (s *Server) lock(ctx context.Context, dirPath, contents, ttl string) (topo.LockDescriptor, error) {
	lockPath := path.Join(s.root, dirPath, locksFilename)

	lockOpts := &api.LockOptions{
		Key:   lockPath,
		Value: []byte(contents),
		SessionOpts: &api.SessionEntry{
			Name: api.DefaultLockSessionName,
			TTL:  api.DefaultLockSessionTTL,
		},
	}
	lockOpts.SessionOpts.Checks = s.lockChecks
	if s.lockTTL != "" {
		// Override the API default with the global default from
		// --topo_consul_lock_session_ttl.
		lockOpts.SessionOpts.TTL = s.lockTTL
	}
	if ttl != "" {
		// Override the global default with the one provided by the
		// caller.
		lockOpts.SessionOpts.TTL = ttl
	}
	if s.lockDelay > 0 {
		lockOpts.SessionOpts.LockDelay = s.lockDelay
	}
	// Build the lock structure.
	l, err := s.client.LockOpts(lockOpts)
	if err != nil {
		return nil, err
	}

	// Wait until we are the only ones in this client trying to
	// lock that path.
	s.mu.Lock()
	li, ok := s.locks[lockPath]
	for ok {
		// Unlock, wait for something to change.
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, convertError(ctx.Err(), dirPath)
		case <-li.done:
		}

		// The original locker is gone, try to get it again
		s.mu.Lock()
		li, ok = s.locks[lockPath]
	}
	li = &lockInstance{
		lock: l,
		done: make(chan struct{}),
	}
	s.locks[lockPath] = li
	s.mu.Unlock()

	// We are the only ones trying to lock now.
	lost, err := l.Lock(ctx.Done())
	if err != nil || lost == nil {
		// Failed to lock, give up our slot in locks map.
		// Close the channel to unblock anyone else.
		s.mu.Lock()
		delete(s.locks, lockPath)
		s.mu.Unlock()
		close(li.done)
		// Consul will return empty leaderCh with nil error if we cannot get lock before the timeout
		// therefore we return a timeout error here
		if lost == nil {
			return nil, topo.NewError(topo.Timeout, lockPath)
		}
		return nil, err
	}

	// We got the lock, we're good.
	return &consulLockDescriptor{
		s:        s,
		lockPath: lockPath,
		lost:     lost,
	}, nil
}

// Check is part of the topo.LockDescriptor interface.
func (ld *consulLockDescriptor) Check(ctx context.Context) error {
	select {
	case <-ld.lost:
		return vterrors.Errorf(vtrpc.Code_INTERNAL, "lost channel closed")
	default:
	}
	return nil
}

// Unlock is part of the topo.LockDescriptor interface.
func (ld *consulLockDescriptor) Unlock(ctx context.Context) error {
	return ld.s.unlock(ctx, ld.lockPath)
}

// unlock releases a lock acquired by Lock() on the given directory.
func (s *Server) unlock(ctx context.Context, lockPath string) error {
	s.mu.Lock()
	li, ok := s.locks[lockPath]
	s.mu.Unlock()
	if !ok {
		return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "unlock: lock %v not held", lockPath)
	}

	// Try to unlock our lock. We will clean up our entry anyway.
	unlockErr := li.lock.Unlock()

	s.mu.Lock()
	delete(s.locks, lockPath)
	s.mu.Unlock()
	close(li.done)

	// Then try to remove the lock entirely. This will only work if
	// no one else has the lock. Use a fresh context: the caller's has
	// often expired by now, and a lock file left behind blocks TryLock.
	destroyCtx, cancel := context.WithTimeout(context.Background(), topo.RemoteOperationTimeout)
	defer cancel()
	if err := s.destroyLockFile(destroyCtx, lockPath); err != nil {
		log.Warningf("failed to clean up lock file %v: %v", lockPath, err)
	}

	return unlockErr
}

// destroyLockFile deletes the lock file at lockPath if no session holds it.
// It mirrors api.Lock.Destroy but goes through s.kv, so transient errors
// (e.g. during a consul leader election) are retried.
func (s *Server) destroyLockFile(ctx context.Context, lockPath string) error {
	pair, _, err := s.kv.Get(lockPath, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to read lock: %w", err)
	}
	if pair == nil {
		return nil
	}
	if pair.Flags != api.LockFlagValue {
		return api.ErrLockConflict
	}
	// If someone else has the lock, we can't remove it, but we don't need to.
	if pair.Session != "" {
		return nil
	}

	ops := api.KVTxnOps{
		&api.KVTxnOp{
			Verb:  api.KVDeleteCAS,
			Key:   lockPath,
			Index: pair.ModifyIndex,
		},
	}
	// A failed CAS means someone else modified the lock file in between,
	// so it is no longer ours to remove.
	if _, _, _, err := s.kv.Txn(ops, (&api.QueryOptions{}).WithContext(ctx)); err != nil {
		return fmt.Errorf("failed to remove lock: %w", err)
	}
	return nil
}
