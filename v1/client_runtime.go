package kmtproto

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// RunChannel drives a client connection from HELLO/RESUME until cancellation,
// idle timeout, transport failure, or a fatal protocol error.
func (c *Client) RunChannel(ctx context.Context, ch Channel, admission Frame) error {
	defer ch.Close()
	if admission.Type != FrameHello && admission.Type != FrameResume {
		return errors.New("client admission must be HELLO or RESUME")
	}
	if err := ch.WriteFrame(ctx, admission); err != nil {
		return err
	}

	in := make(chan Frame, 1)
	readErr := make(chan error, 1)
	go func() {
		for {
			f, err := ch.ReadFrame(ctx)
			if err != nil {
				readErr <- err
				return
			}
			select {
			case in <- f:
			case <-ctx.Done():
				return
			}
		}
	}()

	heartbeat := time.NewTimer(time.Hour)
	idle := time.NewTimer(time.Hour)
	renew := time.NewTimer(time.Hour)
	stopTimer(heartbeat)
	stopTimer(idle)
	stopTimer(renew)
	defer heartbeat.Stop()
	defer idle.Stop()
	defer renew.Stop()
	var nonce atomic.Uint64
	configured := false

	for {
		select {
		case <-ctx.Done():
			c.Disconnect()
			return ctx.Err()
		case err := <-readErr:
			c.Disconnect()
			return err
		case f := <-in:
			out, err := c.Receive(f)
			if err != nil {
				c.Disconnect()
				return err
			}
			for _, reply := range out {
				if err := ch.WriteFrame(ctx, reply); err != nil {
					return err
				}
			}
			c.mu.Lock()
			limits := c.Limits
			ready := c.State == ClientReady
			c.mu.Unlock()
			if limits.IdleTimeoutMS > 0 {
				resetTimer(idle, time.Duration(limits.IdleTimeoutMS)*time.Millisecond)
			}
			if !configured && limits.HeartbeatMS > 0 {
				configured = true
				resetTimer(heartbeat, time.Duration(limits.HeartbeatMS)*time.Millisecond)
				resetTimer(renew, limits.RenewalInterval())
			}
			if ready && configured { /* timers remain authoritative */
			}
		case <-heartbeat.C:
			n := nonce.Add(1)
			ping, err := c.Ping(n, false)
			if err != nil {
				return err
			}
			if err = ch.WriteFrame(ctx, ping); err != nil {
				return err
			}
			c.mu.Lock()
			d := time.Duration(c.Limits.HeartbeatMS) * time.Millisecond
			c.mu.Unlock()
			resetTimer(heartbeat, d)
		case <-renew.C:
			n := nonce.Add(1)
			ping, err := c.Ping(n, true)
			if err != nil {
				return err
			}
			if err = ch.WriteFrame(ctx, ping); err != nil {
				return err
			}
			c.mu.Lock()
			d := c.Limits.RenewalInterval()
			c.mu.Unlock()
			resetTimer(renew, d)
		case <-idle.C:
			c.Disconnect()
			return errors.New("channel idle timeout")
		}
	}
}

func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
func resetTimer(t *time.Timer, d time.Duration) { stopTimer(t); t.Reset(d) }
