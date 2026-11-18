package kmtproto

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// Channel is the reliable, ordered, complete-frame boundary required by V1.
type Channel interface {
	ReadFrame(context.Context) (Frame, error)
	WriteFrame(context.Context, Frame) error
	Close() error
}

type TCPChannel struct {
	conn    net.Conn
	max     uint32
	writeMu sync.Mutex
}

func NewTCPChannel(conn net.Conn, max uint32) *TCPChannel {
	if max < 4096 || max > MaxFrameBytes {
		max = MaxFrameBytes
	}
	return &TCPChannel{conn: conn, max: max}
}
func (c *TCPChannel) Close() error { return c.conn.Close() }
func (c *TCPChannel) ReadFrame(ctx context.Context) (Frame, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetReadDeadline(deadline)
	} else {
		_ = c.conn.SetReadDeadline(time.Time{})
	}
	var h [4]byte
	if _, err := io.ReadFull(c.conn, h[:]); err != nil {
		return Frame{}, err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > c.max {
		return Frame{}, errors.New("invalid TCP frame length")
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(c.conn, b); err != nil {
		return Frame{}, err
	}
	return Decode(b)
}
func (c *TCPChannel) WriteFrame(ctx context.Context, f Frame) error {
	b, err := Encode(f)
	if err != nil {
		return err
	}
	if uint32(len(b)) > c.max {
		return errors.New("frame exceeds channel limit")
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetWriteDeadline(deadline)
	} else {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	if _, err = c.conn.Write(h[:]); err != nil {
		return err
	}
	_, err = c.conn.Write(b)
	return err
}

// ServeChannel drives one admitted connection until close or protocol failure.
func (s *Server) ServeChannel(ctx context.Context, ch Channel, connectionID, principal string) error {
	defer ch.Close()
	for {
		readCtx, cancel := context.WithTimeout(ctx, time.Duration(s.Limits.IdleTimeoutMS)*time.Millisecond)
		f, err := ch.ReadFrame(readCtx)
		cancel()
		if err != nil {
			return err
		}
		out, err := s.Handle(ctx, connectionID, principal, f)
		if err != nil {
			return err
		}
		for _, reply := range out {
			if err := ch.WriteFrame(ctx, reply); err != nil {
				return err
			}
		}
	}
}
