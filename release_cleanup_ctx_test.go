package ads

import (
	"context"
	"testing"
	"time"
)

// release_cleanup_ctx_test.go — the context a batch uses to release PLC handles
// it declined to bind.
//
// The handles exist on the PLC regardless of what the caller's context is doing,
// and nothing else will ever clean them up: they are by definition absent from
// activeNotifications, so Close's releasePLCResources cannot see them either. A
// release that fails before it is sent leaks a subscription streaming to nobody
// until the PLC's route-idle timeout (~10 min), which is how the TwinCAT handle
// table fills up.

// TestReleaseCleanupCtx_PassesThroughLiveCaller: the common case must not add a
// timer or shorten the caller's own budget.
func TestReleaseCleanupCtx_PassesThroughLiveCaller(t *testing.T) {
	sess := newNotifTestSession()
	ctx := context.Background()

	got, cancel := sess.releaseCleanupCtx(ctx)
	defer cancel()
	if got != ctx {
		t.Error("a live caller context was replaced; the release should use it as-is")
	}
}

// TestReleaseCleanupCtx_ExpiredCallerGetsUsableCtx: the deadline the caller gave
// up on says nothing about whether the PLC still holds the handles.
func TestReleaseCleanupCtx_ExpiredCallerGetsUsableCtx(t *testing.T) {
	sess := newNotifTestSession()
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()

	got, cancel := sess.releaseCleanupCtx(expired)
	defer cancel()
	if err := got.Err(); err != nil {
		t.Fatalf("replacement context is already done (%v) — the delete fails before it is sent", err)
	}
	if _, ok := got.Deadline(); !ok {
		t.Error("replacement context has no deadline; a closing session could block on the release")
	}
}

// TestReleaseCleanupCtx_SurvivesClosingSession is the shape that actually leaks.
// A batch aborts BECAUSE the session is shutting down, and the caller's context
// is derived from the lifecycle one (resubscribeNotifications passes it
// directly), so both are done. Deriving the replacement from a cancelled
// lifecycle context yields a context that is born dead — exactly the case this
// replacement exists for.
func TestReleaseCleanupCtx_SurvivesClosingSession(t *testing.T) {
	sess := newNotifTestSession()

	sess.lifecycle.ctxMu.Lock()
	lifeCtx, cancelLife := context.WithCancel(context.Background())
	sess.lifecycle.ctx, sess.lifecycle.shutdown = lifeCtx, cancelLife
	sess.lifecycle.ctxMu.Unlock()
	cancelLife() // session closing

	got, cancel := sess.releaseCleanupCtx(lifeCtx)
	defer cancel()
	if err := got.Err(); err != nil {
		t.Fatalf("release context inherited the closing session's cancellation (%v); the refused handles leak until route-idle timeout", err)
	}
	deadline, ok := got.Deadline()
	if !ok {
		t.Fatal("no deadline: a closing session must not block on cleanup")
	}
	if remaining := time.Until(deadline); remaining > notificationReleaseTimeout+time.Second {
		t.Errorf("deadline is %v away, want at most %v", remaining, notificationReleaseTimeout)
	}
}

// TestReleaseCleanupCtx_NilLifecycleCtxDoesNotPanic: Session struct literals with
// no lifecycle context are a shape this codebase already handles defensively
// (see tearDownAndReset's parent fallback). context.WithTimeout panics on a nil
// parent, so a batch reaching cleanup on such a session would take the process
// down.
func TestReleaseCleanupCtx_NilLifecycleCtxDoesNotPanic(t *testing.T) {
	sess := &Session{
		lifecycle:     &sessionLifecycle{closedCh: make(chan struct{})},
		notifications: &notificationManager{},
		logger:        getDefaultLogger(),
	}
	expired, cancelExpired := context.WithCancel(context.Background())
	cancelExpired()

	got, cancel := sess.releaseCleanupCtx(expired)
	defer cancel()
	if got == nil {
		t.Fatal("nil context returned")
	}
	if err := got.Err(); err != nil {
		t.Errorf("replacement context is done (%v) on a session with no lifecycle context", err)
	}
}
