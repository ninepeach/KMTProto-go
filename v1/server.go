package kmtproto

import (
	"context"
	"crypto/rand"
	"errors"
	"sort"
	"sync"
	"time"
)

type ConnectionState uint8

const (
	ChannelReady ConnectionState = iota
	Recovering
	Ready
	Closed
)

type Authorizer interface {
	SelectStream(context.Context, string, string) (string, error)
	AuthorizeSession(context.Context, string, Session) error
	Reauthorize(context.Context, string, Session) error
}
type StateAuthorizer interface {
	AuthorizeStateKeys(context.Context, string, Session, []StateKey) error
}
type SendHandler func(context.Context, Session, Send) (uint8, []byte, error)
type RequestHandler func(context.Context, Session, Request) (uint8, []byte, error)
type RateLimitPolicy interface {
	RetryAfter(context.Context, Session, FrameType) time.Duration
}
type FixedRateLimit time.Duration

func (f FixedRateLimit) RetryAfter(context.Context, Session, FrameType) time.Duration {
	return time.Duration(f)
}

type sconn struct {
	session         Session
	state           ConnectionState
	caps            map[uint16]bool
	replayTo        uint64
	lastRenewal     time.Time
	syncID          ID
	syncBaseline    uint64
	inflightSend    uint16
	inflightRequest uint16
	inflightQuery   uint16
}
type Server struct {
	Store            Storage
	Auth             Authorizer
	Limits           Limits
	AbsoluteLifetime time.Duration
	HandleSend       SendHandler
	HandleRequest    RequestHandler
	RateLimit        RateLimitPolicy
	Now              func() time.Time
	mu               sync.Mutex
	conns            map[string]*sconn
	owners           map[ID]string
}

func NewServer(st Storage, a Authorizer) *Server {
	if st == nil {
		st = NewMemoryStore()
	}
	return &Server{Store: st, Auth: a, Limits: DefaultLimits(), AbsoluteLifetime: time.Hour, RateLimit: FixedRateLimit(100 * time.Millisecond), Now: time.Now, conns: map[string]*sconn{}, owners: map[ID]string{}}
}
func NewID() (ID, error) { var v ID; _, e := rand.Read(v[:]); return v, e }
func pe(c uint16, r ErrorRef) Frame {
	return Frame{FrameError, ErrorFrame{Code: c, Retryable: ErrorRetryable(c), Ref: r}}
}
func (s *Server) rateLimited(ctx context.Context, session Session, kind FrameType, ref ErrorRef) Frame {
	delay := time.Millisecond
	if s.RateLimit != nil {
		if d := s.RateLimit.RetryAfter(ctx, session, kind); d > 0 {
			delay = d
		}
	}
	ms := delay.Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if ms > int64(^uint32(0)) {
		ms = int64(^uint32(0))
	}
	f := pe(ErrRateLimited, ref)
	body := f.Body.(ErrorFrame)
	body.RetryAfterMS = uint32(ms)
	f.Body = body
	return f
}
func cm(v []uint16) map[uint16]bool {
	m := map[uint16]bool{}
	for _, x := range v {
		m[x] = true
	}
	return m
}
func cs(m map[uint16]bool) []uint16 {
	o := []uint16{}
	for x := range m {
		o = append(o, x)
	}
	sort.Slice(o, func(i, j int) bool { return o[i] < o[j] })
	return o
}
func (s *Server) eff(c ClientLimits) Limits {
	v := s.Limits
	if c.MaxFrameBytes < v.MaxFrameBytes {
		v.MaxFrameBytes = c.MaxFrameBytes
	}
	return v
}
func (s *Server) Handle(ctx context.Context, cid, p string, f Frame) ([]Frame, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.conns[cid]
	if c == nil {
		if f.Type == FrameHello {
			return s.hello(ctx, cid, p, f.Body.(Hello))
		}
		if f.Type == FrameResume {
			return s.resume(ctx, cid, p, f.Body.(Resume))
		}
		return []Frame{pe(ErrProtocolState, ErrorRef{})}, nil
	}
	if c.state == Closed || s.owners[c.session.ID] != cid {
		return nil, ErrFenced
	}
	now := s.Now()
	if !now.Before(c.session.ExpiresAt) || !now.Before(c.session.AbsoluteExpiresAt) {
		c.state = Closed
		return []Frame{pe(ErrSessionExpired, ErrorRef{})}, nil
	}
	if c.state == Recovering {
		if f.Type == FrameSyncCommit {
			return s.finishSync(ctx, c, f.Body.(SyncCommit))
		}
		if f.Type == FramePing {
			return s.ping(ctx, p, c, f.Body.(Ping))
		}
		return []Frame{pe(ErrProtocolState, ErrorRef{})}, nil
	}
	switch f.Type {
	case FramePing:
		return s.ping(ctx, p, c, f.Body.(Ping))
	case FrameSend:
		return s.send(ctx, c, f.Body.(Send))
	case FrameRequest:
		return s.request(ctx, c, f.Body.(Request))
	case FrameStateQuery:
		return s.query(ctx, c, f.Body.(StateQuery))
	default:
		return []Frame{pe(ErrProtocolState, ErrorRef{})}, nil
	}
}
func (s *Server) hello(ctx context.Context, cid, p string, h Hello) ([]Frame, error) {
	if s.Auth == nil {
		return []Frame{pe(ErrUnauthorized, ErrorRef{})}, nil
	}
	stream, e := s.Auth.SelectStream(ctx, p, h.StreamID)
	if e != nil {
		return []Frame{pe(ErrUnauthorized, ErrorRef{})}, nil
	}
	now := s.Now()
	id, e := NewID()
	if e != nil {
		return nil, e
	}
	head, _ := s.Store.StreamHead(ctx, stream)
	l := s.eff(h.Limits)
	ss := Session{ID: id, Principal: p, StreamID: stream, Generation: 1, ExpiresAt: now.Add(time.Duration(l.SessionTTLSeconds) * time.Second), AbsoluteExpiresAt: now.Add(s.AbsoluteLifetime), LastRenewalAt: now, MaxFrameFloor: l.MaxFrameBytes, MaxInflightSendFloor: l.MaxInflightSend}
	if e = s.Store.CreateSession(ctx, ss); e != nil {
		return nil, e
	}
	caps := cm(NegotiateCapabilities(h.Capabilities, []uint16{1}))
	s.conns[cid] = &sconn{session: ss, state: Ready, caps: caps, replayTo: head, lastRenewal: now}
	s.owners[id] = cid
	if h.StateSyncRequested {
		return s.fullSync(ctx, cid, ss, caps, l)
	}
	return []Frame{{FrameWelcome, Welcome{id, stream, AdmissionNew, 1, head, l, cs(caps)}}}, nil
}
func (s *Server) resume(ctx context.Context, cid, p string, r Resume) ([]Frame, error) {
	old, e := s.Store.GetSession(ctx, r.SessionID)
	if e != nil || s.Auth == nil || s.Auth.AuthorizeSession(ctx, p, old) != nil {
		return []Frame{pe(ErrUnauthorized, ErrorRef{})}, nil
	}
	if r.Limits.MaxFrameBytes < old.MaxFrameFloor {
		return []Frame{pe(ErrLimitExceeded, ErrorRef{})}, nil
	}
	now := s.Now()
	ss, ev, e := s.Store.AdmitResume(ctx, r.SessionID, p, r.Cursor.StreamID, r.Cursor.LastSeq, now, time.Duration(s.Limits.SessionTTLSeconds)*time.Second)
	if errors.Is(e, ErrExpired) {
		return []Frame{pe(ErrSessionExpired, ErrorRef{})}, nil
	}
	if e != nil {
		return []Frame{pe(ErrAdmissionRejected, ErrorRef{})}, nil
	}
	if r.StateSyncRequested {
		return s.fullSync(ctx, cid, ss, cm(NegotiateCapabilities(r.Capabilities, []uint16{1})), s.eff(r.Limits))
	}
	l := s.eff(r.Limits)
	caps := cm(NegotiateCapabilities(r.Capabilities, []uint16{1}))
	c := &sconn{session: ss, state: Recovering, caps: caps, replayTo: r.Cursor.LastSeq + uint64(len(ev)), lastRenewal: now}
	if oldc := s.conns[s.owners[ss.ID]]; oldc != nil {
		oldc.state = Closed
	}
	s.conns[cid] = c
	s.owners[ss.ID] = cid
	o := []Frame{{FrameWelcome, Welcome{ss.ID, ss.StreamID, AdmissionResumed, ss.Generation, c.replayTo, l, cs(caps)}}}
	partial := uint64(len(ev)) > uint64(l.MaxReplayEvents)
	if partial {
		ev = ev[:l.MaxReplayEvents]
	}
	for _, x := range ev {
		o = append(o, Frame{FrameEvent, x})
	}
	if partial {
		c.state = Closed
		return append(o, pe(ErrReplayPartial, ErrorRef{})), nil
	}
	o = append(o, Frame{FrameRecoveryComplete, RecoveryComplete{c.replayTo}})
	c.state = Ready
	return o, nil
}
func (s *Server) fullSync(ctx context.Context, cid string, ss Session, caps map[uint16]bool, l Limits) ([]Frame, error) {
	base, scope, entries, e := s.Store.FullSyncView(ctx, ss.StreamID)
	if e != nil {
		return []Frame{pe(ErrAdmissionRejected, ErrorRef{})}, nil
	}
	id, e := NewID()
	if e != nil {
		return nil, e
	}
	c := &sconn{session: ss, state: Recovering, caps: caps, replayTo: base, lastRenewal: s.Now(), syncID: id, syncBaseline: base}
	s.conns[cid] = c
	s.owners[ss.ID] = cid
	o := []Frame{{FrameWelcome, Welcome{ss.ID, ss.StreamID, AdmissionFullSync, ss.Generation, base, l, cs(caps)}}, {FrameSyncBegin, SyncBegin{id, base, scope, uint32(len(entries))}}}
	if len(entries) == 0 {
		return append(o, Frame{FrameSyncPage, SyncPage{id, 0, true, nil}}), nil
	}
	for i := 0; i < len(entries); i += 64 {
		n := i + 64
		if n > len(entries) {
			n = len(entries)
		}
		o = append(o, Frame{FrameSyncPage, SyncPage{id, uint32(i / 64), n == len(entries), entries[i:n]}})
	}
	return o, nil
}
func (s *Server) finishSync(ctx context.Context, c *sconn, v SyncCommit) ([]Frame, error) {
	if v.SyncID != c.syncID || v.BaselineSeq != c.syncBaseline {
		c.state = Closed
		return []Frame{pe(ErrSyncAborted, ErrorRef{})}, nil
	}
	c.state = Ready
	out := []Frame{{FrameRecoveryComplete, RecoveryComplete{c.syncBaseline}}}
	events, err := s.Store.EventsAfter(ctx, c.session.StreamID, c.syncBaseline)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		out = append(out, Frame{FrameEvent, e})
	}
	return out, nil
}
func (s *Server) send(ctx context.Context, c *sconn, v Send) ([]Frame, error) {
	if c.inflightSend >= s.Limits.MaxInflightSend {
		return []Frame{s.rateLimited(ctx, c.session, FrameSend, ErrorRef{1, v.MsgID})}, nil
	}
	r, fresh, e := s.Store.ClaimSend(ctx, c.session.ID, c.session.Generation, v)
	if errors.Is(e, ErrConflict) {
		return []Frame{pe(ErrSendConflict, ErrorRef{1, v.MsgID})}, nil
	}
	if e != nil {
		return nil, e
	}
	if !fresh {
		if r.State == DedupCommitted {
			return []Frame{{FrameAck, r.Ack}}, nil
		}
		if r.State == DedupClaimed {
			return []Frame{pe(ErrSendPending, ErrorRef{1, v.MsgID})}, nil
		}
		return []Frame{pe(ErrSendIndeterminate, ErrorRef{1, v.MsgID})}, nil
	}
	o := []Frame{}
	if c.caps[1] {
		o = append(o, Frame{FrameQuickAck, QuickAck{v.MsgID}})
	}
	if s.HandleSend == nil {
		_ = s.Store.MarkIndeterminate(ctx, c.session.ID, v.MsgID)
		return append(o, pe(ErrSendIndeterminate, ErrorRef{1, v.MsgID})), nil
	}
	g := c.session.Generation
	c.inflightSend++
	s.mu.Unlock()
	out, res, e := s.HandleSend(ctx, c.session, v)
	s.mu.Lock()
	c.inflightSend--
	if e != nil {
		_ = s.Store.MarkIndeterminate(ctx, c.session.ID, v.MsgID)
		return append(o, pe(ErrSendIndeterminate, ErrorRef{1, v.MsgID})), nil
	}
	a := Ack{v.MsgID, out, res}
	if e = s.Store.CommitSend(ctx, c.session.ID, g, a); e != nil {
		_ = s.Store.MarkIndeterminate(ctx, c.session.ID, v.MsgID)
		return append(o, pe(ErrSendIndeterminate, ErrorRef{1, v.MsgID})), nil
	}
	return append(o, Frame{FrameAck, a}), nil
}
func (s *Server) request(ctx context.Context, c *sconn, v Request) ([]Frame, error) {
	if c.inflightRequest >= s.Limits.MaxInflightRequest {
		return []Frame{s.rateLimited(ctx, c.session, FrameRequest, ErrorRef{1, v.RequestID})}, nil
	}
	if s.HandleRequest == nil {
		return []Frame{{FrameResponse, Response{v.RequestID, OutcomeApplicationError, nil}}}, nil
	}
	c.inflightRequest++
	s.mu.Unlock()
	o, r, e := s.HandleRequest(ctx, c.session, v)
	s.mu.Lock()
	c.inflightRequest--
	if e != nil {
		o = OutcomeApplicationError
		r = []byte(e.Error())
	}
	return []Frame{{FrameResponse, Response{v.RequestID, o, r}}}, nil
}
func (s *Server) query(ctx context.Context, c *sconn, q StateQuery) ([]Frame, error) {
	if c.inflightQuery >= s.Limits.MaxInflightQuery {
		return []Frame{s.rateLimited(ctx, c.session, FrameStateQuery, ErrorRef{2, q.QueryID})}, nil
	}
	if len(q.Keys) > int(s.Limits.MaxStateItems) {
		return []Frame{pe(ErrLimitExceeded, ErrorRef{2, q.QueryID})}, nil
	}
	if a, ok := s.Auth.(StateAuthorizer); ok && a.AuthorizeStateKeys(ctx, c.session.Principal, c.session, q.Keys) != nil {
		return []Frame{pe(ErrStateQueryFailed, ErrorRef{2, q.QueryID})}, nil
	}
	c.inflightQuery++
	r, e := s.Store.QueryState(ctx, c.session.StreamID, q.Keys)
	c.inflightQuery--
	if e != nil {
		return []Frame{pe(ErrStateQueryFailed, ErrorRef{2, q.QueryID})}, nil
	}
	out := []Frame{}
	page := int(s.Limits.MaxStateItems)
	if page > 64 {
		page = 64
	}
	for i := 0; i < len(r); i += page {
		n := i + page
		if n > len(r) {
			n = len(r)
		}
		out = append(out, Frame{FrameStateSnapshot, StateSnapshot{q.QueryID, uint32(i / page), n == len(r), r[i:n]}})
	}
	return out, nil
}
func (s *Server) ping(ctx context.Context, p string, c *sconn, v Ping) ([]Frame, error) {
	if !v.Renew {
		return []Frame{{FramePong, Pong{v.Nonce}}}, nil
	}
	now := s.Now()
	if now.Before(c.lastRenewal.Add(s.Limits.RenewalInterval())) {
		c.state = Closed
		return []Frame{pe(ErrProtocolState, ErrorRef{})}, nil
	}
	if s.Auth.Reauthorize(ctx, p, c.session) != nil {
		c.state = Closed
		return []Frame{pe(ErrAuthRevoked, ErrorRef{})}, nil
	}
	ss, e := s.Store.Renew(ctx, c.session.ID, c.session.Generation, now, time.Duration(s.Limits.SessionTTLSeconds)*time.Second)
	if e != nil {
		c.state = Closed
		return []Frame{pe(ErrSessionExpired, ErrorRef{})}, nil
	}
	c.session = ss
	c.lastRenewal = now
	return []Frame{{FramePong, Pong{v.Nonce}}}, nil
}
func (s *Server) Publish(ctx context.Context, stream string, b []byte) (Event, error) {
	id, e := NewID()
	if e != nil {
		return Event{}, e
	}
	return s.Store.AppendEvent(ctx, stream, Event{id, 0, b})
}
