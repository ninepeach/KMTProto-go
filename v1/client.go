package kmtproto

import (
	"bytes"
	"errors"
	"sync"
)

type ClientState uint8

const (
	ClientDisconnected ClientState = iota
	ClientHandshaking
	ClientRecovering
	ClientReady
	ClientClosed
)

type DurableClientStore interface {
	Cursor() Cursor
	SetCursor(Cursor) error
	PutOutbox(ID, []byte) error
	DeleteOutbox(ID) error
	ReplaceState(string, []StateEntry, Cursor) error
}
type MemoryClientStore struct {
	mu     sync.Mutex
	cursor Cursor
	outbox map[ID][]byte
	states map[string]map[StateKey]StateEntry
}

func NewMemoryClientStore() *MemoryClientStore {
	return &MemoryClientStore{outbox: map[ID][]byte{}, states: map[string]map[StateKey]StateEntry{}}
}
func (m *MemoryClientStore) Cursor() Cursor { m.mu.Lock(); defer m.mu.Unlock(); return m.cursor }
func (m *MemoryClientStore) SetCursor(c Cursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cursor.StreamID != "" && m.cursor.StreamID != c.StreamID || c.LastSeq < m.cursor.LastSeq {
		return ErrConflict
	}
	m.cursor = c
	return nil
}
func (m *MemoryClientStore) PutOutbox(id ID, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if x, ok := m.outbox[id]; ok && !bytes.Equal(x, b) {
		return ErrConflict
	}
	m.outbox[id] = bytes.Clone(b)
	return nil
}
func (m *MemoryClientStore) DeleteOutbox(id ID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.outbox, id)
	return nil
}
func (m *MemoryClientStore) ReplaceState(scope string, v []StateEntry, c Cursor) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	x := map[StateKey]StateEntry{}
	for _, e := range v {
		x[StateKey{e.Namespace, e.ObjectID}] = e
	}
	m.states[scope] = x
	m.cursor = c
	return nil
}

type psync struct {
	id      ID
	scope   string
	base    uint64
	total   uint32
	page    uint32
	entries []StateEntry
	done    bool
}
type pquery struct {
	keys    []StateKey
	page    uint32
	results []SnapshotResult
}
type Client struct {
	mu             sync.Mutex
	State          ClientState
	SessionID      ID
	Generation     uint64
	Limits         Limits
	Store          DurableClientStore
	caps           map[uint16]bool
	next, replayTo uint64
	pings          map[uint64]bool
	queries        map[ID]*pquery
	sync           *psync
	OnEvent        func(Event) error
	OnFullSync     func(string, uint64)
}

func NewClient(st DurableClientStore) *Client {
	if st == nil {
		st = NewMemoryClientStore()
	}
	return &Client{State: ClientDisconnected, Store: st, caps: map[uint16]bool{}, pings: map[uint64]bool{}, queries: map[ID]*pquery{}}
}
func (c *Client) Hello(stream string, attach, sync bool) Frame {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.State = ClientHandshaking
	m := uint8(0)
	if attach {
		m = 1
	}
	cur := c.Store.Cursor()
	return Frame{FrameHello, Hello{m, stream, []uint16{1}, ClientLimits{MaxFrameBytes, 65536}, cur.LastSeq, sync}}
}
func (c *Client) Resume(sync bool) (Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.SessionID.IsZero() {
		return Frame{}, errors.New("no session")
	}
	c.State = ClientRecovering
	return Frame{FrameResume, Resume{c.SessionID, c.Store.Cursor(), []uint16{1}, ClientLimits{MaxFrameBytes, 65536}, sync}}, nil
}
func (c *Client) Send(b []byte) (Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.State != ClientReady {
		return Frame{}, errors.New("not ready")
	}
	id, e := NewID()
	if e != nil {
		return Frame{}, e
	}
	f := Frame{FrameSend, Send{id, bytes.Clone(b)}}
	raw, e := Encode(f)
	if e != nil || uint32(len(raw)) > c.Limits.MaxFrameBytes {
		return Frame{}, errors.New("SEND too large")
	}
	if e = c.Store.PutOutbox(id, b); e != nil {
		return Frame{}, e
	}
	return f, nil
}
func (c *Client) Ping(n uint64, r bool) (Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if (c.State != ClientReady && c.State != ClientRecovering) || c.pings[n] {
		return Frame{}, errors.New("PING not admitted")
	}
	c.pings[n] = r
	return Frame{FramePing, Ping{n, r}}, nil
}
func (c *Client) Query(k []StateKey) (Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.State != ClientReady {
		return Frame{}, errors.New("not ready")
	}
	id, e := NewID()
	if e != nil {
		return Frame{}, e
	}
	c.queries[id] = &pquery{keys: append([]StateKey(nil), k...)}
	return Frame{FrameStateQuery, StateQuery{id, k}}, nil
}
func (c *Client) Receive(f Frame) ([]Frame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch v := f.Body.(type) {
	case Welcome:
		if c.State != ClientHandshaking && c.State != ClientRecovering {
			return nil, errors.New("WELCOME state")
		}
		if e := validateLimits(v.Limits); e != nil {
			return nil, e
		}
		c.SessionID = v.SessionID
		c.Generation = v.Generation
		c.Limits = v.Limits
		c.caps = cm(v.Capabilities)
		c.replayTo = v.ReplayTo
		if v.Admission == AdmissionNew {
			if e := c.Store.SetCursor(Cursor{v.StreamID, v.ReplayTo}); e != nil {
				return nil, e
			}
			c.next = v.ReplayTo + 1
			c.State = ClientReady
		} else {
			c.next = c.Store.Cursor().LastSeq + 1
			c.State = ClientRecovering
		}
	case Event:
		if c.State != ClientReady && c.State != ClientRecovering || c.sync != nil || v.Seq != c.next {
			return nil, errors.New("EVENT gap/state")
		}
		if c.OnEvent != nil {
			if e := c.OnEvent(v); e != nil {
				return nil, e
			}
		}
		cur := c.Store.Cursor()
		cur.LastSeq = v.Seq
		if e := c.Store.SetCursor(cur); e != nil {
			return nil, e
		}
		c.next++
	case RecoveryComplete:
		if c.State != ClientRecovering || v.ReplayTo != c.replayTo || c.Store.Cursor().LastSeq != v.ReplayTo {
			return nil, errors.New("incomplete recovery")
		}
		c.State = ClientReady
		c.next = v.ReplayTo + 1
	case Ack:
		if e := c.Store.DeleteOutbox(v.MsgID); e != nil {
			return nil, e
		}
	case QuickAck:
		if !c.caps[1] {
			return nil, errors.New("QUICK_ACK not negotiated")
		}
	case Pong:
		delete(c.pings, v.Nonce)
	case StateSnapshot:
		q := c.queries[v.QueryID]
		if q == nil || q.page != v.PageIndex {
			return nil, errors.New("bad snapshot page")
		}
		if len(q.results)+len(v.Results) > len(q.keys) {
			return nil, errors.New("too many results")
		}
		for i, r := range v.Results {
			k := q.keys[len(q.results)+i]
			if r.Status != SnapshotMissing && (r.Entry.Namespace != k.Namespace || r.Entry.ObjectID != k.ObjectID) {
				return nil, errors.New("key mismatch")
			}
		}
		q.results = append(q.results, v.Results...)
		q.page++
		if v.FinalPage {
			if len(q.results) != len(q.keys) {
				return nil, errors.New("incomplete query")
			}
			delete(c.queries, v.QueryID)
		}
	case SyncBegin:
		if c.State != ClientRecovering || v.TotalEntries > 65536 {
			return nil, errors.New("bad sync")
		}
		c.sync = &psync{id: v.SyncID, scope: v.ScopeID, base: v.BaselineSeq, total: v.TotalEntries}
	case SyncPage:
		s := c.sync
		if s == nil || s.id != v.SyncID || s.page != v.PageIndex || s.done {
			return nil, errors.New("bad sync page")
		}
		s.entries = append(s.entries, v.Entries...)
		s.page++
		s.done = v.FinalPage
		if s.done {
			if uint32(len(s.entries)) != s.total {
				return nil, errors.New("sync count")
			}
			cur := c.Store.Cursor()
			cur.LastSeq = s.base
			if e := c.Store.ReplaceState(s.scope, s.entries, cur); e != nil {
				return nil, e
			}
			c.next = s.base + 1
			c.replayTo = s.base
			if c.OnFullSync != nil {
				c.OnFullSync(s.scope, s.base)
			}
			return []Frame{{FrameSyncCommit, SyncCommit{s.id, s.base}}}, nil
		}
	case ErrorFrame:
		if v.Code == ErrSessionExpired || v.Code == ErrAuthRevoked || v.Code == ErrReplayPartial {
			c.State = ClientDisconnected
		}
		return nil, errors.New("peer ERROR")
	case Response, StateUpdate:
		if c.State != ClientReady {
			return nil, errors.New("frame not admitted")
		}
	default:
		return nil, errors.New("unexpected frame")
	}
	return nil, nil
}
func (c *Client) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.State = ClientDisconnected
	c.pings = map[uint64]bool{}
	c.queries = map[ID]*pquery{}
	c.sync = nil
}
