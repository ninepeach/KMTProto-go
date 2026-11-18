package kmtproto

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrExpired  = errors.New("expired")
	ErrFenced   = errors.New("fenced")
)

type DedupState uint8

const (
	DedupClaimed DedupState = iota + 1
	DedupCommitted
	DedupIndeterminate
)

type DedupRecord struct {
	Content []byte
	State   DedupState
	Ack     Ack
}
type Session struct {
	ID                                          ID
	Principal, StreamID                         string
	Generation                                  uint64
	ExpiresAt, AbsoluteExpiresAt, LastRenewalAt time.Time
	MaxFrameFloor                               uint32
	MaxInflightSendFloor                        uint16
}
type Storage interface {
	CreateSession(context.Context, Session) error
	AdmitResume(context.Context, ID, string, string, uint64, time.Time, time.Duration) (Session, []Event, error)
	Renew(context.Context, ID, uint64, time.Time, time.Duration) (Session, error)
	GetSession(context.Context, ID) (Session, error)
	ClaimSend(context.Context, ID, uint64, Send) (DedupRecord, bool, error)
	CommitSend(context.Context, ID, uint64, Ack) error
	MarkIndeterminate(context.Context, ID, ID) error
	AppendEvent(context.Context, string, Event) (Event, error)
	StreamHead(context.Context, string) (uint64, error)
	QueryState(context.Context, string, []StateKey) ([]SnapshotResult, error)
	PutState(context.Context, string, StateEntry) error
	FullSyncView(context.Context, string) (uint64, string, []StateEntry, error)
	EventsAfter(context.Context, string, uint64) ([]Event, error)
}

func (m *MemoryStore) EventsAfter(_ context.Context, stream string, after uint64) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.streams[stream]
	if s == nil {
		return nil, ErrNotFound
	}
	out := []Event{}
	for _, e := range s.events {
		if e.Seq > after {
			out = append(out, ce(e))
		}
	}
	return out, nil
}

func (m *MemoryStore) FullSyncView(_ context.Context, stream string) (uint64, string, []StateEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.streams[stream]
	if s == nil {
		return 0, "", nil, ErrNotFound
	}
	out := make([]StateEntry, 0, len(s.states))
	for _, v := range s.states {
		v.Data = bytes.Clone(v.Data)
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Namespace != out[j].Namespace {
			return out[i].Namespace < out[j].Namespace
		}
		return out[i].ObjectID < out[j].ObjectID
	})
	return uint64(len(s.events)), stream, out, nil
}

type msess struct {
	Session
	dedup map[ID]*DedupRecord
}
type mstream struct {
	events []Event
	states map[StateKey]StateEntry
}
type MemoryStore struct {
	mu       sync.Mutex
	sessions map[ID]*msess
	streams  map[string]*mstream
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{sessions: map[ID]*msess{}, streams: map[string]*mstream{}}
}
func ca(a Ack) Ack     { a.Result = bytes.Clone(a.Result); return a }
func ce(e Event) Event { e.Content = bytes.Clone(e.Content); return e }
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (m *MemoryStore) CreateSession(_ context.Context, s Session) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.ID.IsZero() || s.StreamID == "" || s.Generation != 1 {
		return ErrConflict
	}
	if _, ok := m.sessions[s.ID]; ok {
		return ErrConflict
	}
	if m.streams[s.StreamID] == nil {
		m.streams[s.StreamID] = &mstream{states: map[StateKey]StateEntry{}}
	}
	m.sessions[s.ID] = &msess{Session: s, dedup: map[ID]*DedupRecord{}}
	return nil
}
func (m *MemoryStore) GetSession(_ context.Context, id ID) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[id]
	if s == nil {
		return Session{}, ErrNotFound
	}
	return s.Session, nil
}
func (m *MemoryStore) AdmitResume(_ context.Context, id ID, p, stream string, cursor uint64, now time.Time, ttl time.Duration) (Session, []Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[id]
	if s == nil || s.Principal != p {
		return Session{}, nil, ErrNotFound
	}
	if !now.Before(s.ExpiresAt) || !now.Before(s.AbsoluteExpiresAt) {
		return Session{}, nil, ErrExpired
	}
	if s.StreamID != stream {
		return Session{}, nil, ErrConflict
	}
	st := m.streams[stream]
	head := uint64(len(st.events))
	if cursor > head {
		return Session{}, nil, ErrConflict
	}
	s.Generation++
	s.ExpiresAt = minTime(now.Add(ttl), s.AbsoluteExpiresAt)
	s.LastRenewalAt = now
	out := []Event{}
	for _, e := range st.events {
		if e.Seq > cursor {
			out = append(out, ce(e))
		}
	}
	return s.Session, out, nil
}
func (m *MemoryStore) Renew(_ context.Context, id ID, g uint64, now time.Time, ttl time.Duration) (Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[id]
	if s == nil {
		return Session{}, ErrNotFound
	}
	if s.Generation != g {
		return Session{}, ErrFenced
	}
	if !now.Before(s.ExpiresAt) || !now.Before(s.AbsoluteExpiresAt) {
		return Session{}, ErrExpired
	}
	s.ExpiresAt = minTime(now.Add(ttl), s.AbsoluteExpiresAt)
	s.LastRenewalAt = now
	return s.Session, nil
}
func (m *MemoryStore) ClaimSend(_ context.Context, sid ID, g uint64, v Send) (DedupRecord, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s == nil {
		return DedupRecord{}, false, ErrNotFound
	}
	if s.Generation != g {
		return DedupRecord{}, false, ErrFenced
	}
	if r := s.dedup[v.MsgID]; r != nil {
		if !bytes.Equal(r.Content, v.Content) {
			return DedupRecord{}, false, ErrConflict
		}
		x := *r
		x.Content = bytes.Clone(x.Content)
		x.Ack = ca(x.Ack)
		return x, false, nil
	}
	r := &DedupRecord{Content: bytes.Clone(v.Content), State: DedupClaimed}
	s.dedup[v.MsgID] = r
	return *r, true, nil
}
func (m *MemoryStore) CommitSend(_ context.Context, sid ID, g uint64, a Ack) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s == nil {
		return ErrNotFound
	}
	if s.Generation != g {
		return ErrFenced
	}
	r := s.dedup[a.MsgID]
	if r == nil {
		return ErrNotFound
	}
	if r.State == DedupCommitted {
		return nil
	}
	if r.State != DedupClaimed {
		return ErrConflict
	}
	r.State = DedupCommitted
	r.Ack = ca(a)
	return nil
}
func (m *MemoryStore) MarkIndeterminate(_ context.Context, sid, msg ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.sessions[sid]
	if s == nil || s.dedup[msg] == nil {
		return ErrNotFound
	}
	s.dedup[msg].State = DedupIndeterminate
	return nil
}
func (m *MemoryStore) AppendEvent(_ context.Context, stream string, e Event) (Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.streams[stream]
	if s == nil {
		return Event{}, ErrNotFound
	}
	e.Seq = uint64(len(s.events)) + 1
	e = ce(e)
	s.events = append(s.events, e)
	return ce(e), nil
}
func (m *MemoryStore) StreamHead(_ context.Context, stream string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.streams[stream]
	if s == nil {
		return 0, ErrNotFound
	}
	return uint64(len(s.events)), nil
}
func (m *MemoryStore) QueryState(_ context.Context, stream string, keys []StateKey) ([]SnapshotResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.streams[stream]
	if s == nil {
		return nil, ErrNotFound
	}
	o := make([]SnapshotResult, len(keys))
	for i, k := range keys {
		v, ok := s.states[k]
		if !ok {
			o[i].Status = SnapshotMissing
			continue
		}
		v.Data = bytes.Clone(v.Data)
		o[i].Entry = v
		if v.Deleted {
			o[i].Status = SnapshotDeleted
		} else {
			o[i].Status = SnapshotPresent
		}
	}
	return o, nil
}
func (m *MemoryStore) PutState(_ context.Context, stream string, v StateEntry) error {
	if err := validateEntry(v); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.streams[stream]
	if s == nil {
		return ErrNotFound
	}
	k := StateKey{v.Namespace, v.ObjectID}
	old, ok := s.states[k]
	if ok && v.Version < old.Version {
		return ErrConflict
	}
	if ok && v.Version == old.Version && (!bytes.Equal(v.Data, old.Data) || v.Deleted != old.Deleted) {
		return ErrConflict
	}
	v.Data = bytes.Clone(v.Data)
	s.states[k] = v
	return nil
}
