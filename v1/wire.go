package kmtproto

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"unicode/utf8"
)

type enc struct {
	b   []byte
	err error
}

func (e *enc) u8(v uint8) { e.b = append(e.b, v) }
func (e *enc) u16(v uint16) {
	var x [2]byte
	binary.BigEndian.PutUint16(x[:], v)
	e.b = append(e.b, x[:]...)
}
func (e *enc) u32(v uint32) {
	var x [4]byte
	binary.BigEndian.PutUint32(x[:], v)
	e.b = append(e.b, x[:]...)
}
func (e *enc) u64(v uint64) {
	var x [8]byte
	binary.BigEndian.PutUint64(x[:], v)
	e.b = append(e.b, x[:]...)
}
func (e *enc) id(v ID) { e.b = append(e.b, v[:]...) }
func (e *enc) boo(v bool) {
	if v {
		e.u8(1)
	} else {
		e.u8(0)
	}
}
func (e *enc) txt(v string, empty bool) {
	if e.err != nil {
		return
	}
	if (!empty && v == "") || len(v) > math.MaxUint16 || !utf8.ValidString(v) {
		e.err = errors.New("invalid text")
		return
	}
	for _, r := range v {
		if r == 0 {
			e.err = errors.New("text contains NUL")
			return
		}
	}
	e.u16(uint16(len(v)))
	e.b = append(e.b, v...)
}
func (e *enc) bytes(v []byte) {
	if uint64(len(v)) > math.MaxUint32 {
		e.err = errors.New("bytes too long")
		return
	}
	e.u32(uint32(len(v)))
	e.b = append(e.b, v...)
}
func (e *enc) caps(v []uint16) {
	if len(v) > 64 {
		e.err = errors.New("too many capabilities")
		return
	}
	e.u16(uint16(len(v)))
	var p uint16
	for _, c := range v {
		if c == 0 || c <= p {
			e.err = errors.New("capabilities not sorted")
			return
		}
		e.u16(c)
		p = c
	}
}
func (e *enc) cl(v ClientLimits) { e.u32(v.MaxFrameBytes); e.u32(v.MaxSyncEntries) }
func (e *enc) lim(v Limits) {
	e.u32(v.MaxFrameBytes)
	e.u16(v.MaxInflightSend)
	e.u16(v.MaxInflightRequest)
	e.u16(v.MaxInflightQuery)
	e.u32(v.MaxReplayEvents)
	e.u16(v.MaxStateItems)
	e.u32(v.HeartbeatMS)
	e.u32(v.IdleTimeoutMS)
	e.u32(v.SessionTTLSeconds)
	e.u32(v.AbsoluteRemainingSeconds)
}
func (e *enc) key(v StateKey) { e.txt(v.Namespace, false); e.txt(v.ObjectID, false) }
func (e *enc) entry(v StateEntry) {
	e.txt(v.Namespace, false)
	e.txt(v.ObjectID, false)
	e.u64(v.Version)
	e.boo(v.Deleted)
	e.bytes(v.Data)
}

type dec struct {
	b   []byte
	i   int
	err error
}

func (d *dec) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n < 0 || n > len(d.b)-d.i {
		d.err = errors.New("truncated")
		return nil
	}
	v := d.b[d.i : d.i+n]
	d.i += n
	return v
}
func (d *dec) u8() uint8 {
	v := d.take(1)
	if v == nil {
		return 0
	}
	return v[0]
}
func (d *dec) u16() uint16 {
	v := d.take(2)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint16(v)
}
func (d *dec) u32() uint32 {
	v := d.take(4)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint32(v)
}
func (d *dec) u64() uint64 {
	v := d.take(8)
	if v == nil {
		return 0
	}
	return binary.BigEndian.Uint64(v)
}
func (d *dec) id() ID { var v ID; copy(v[:], d.take(16)); return v }
func (d *dec) boo() bool {
	v := d.u8()
	if v > 1 {
		d.err = errors.New("invalid boolean")
	}
	return v == 1
}
func (d *dec) txt(empty bool) string {
	n := int(d.u16())
	v := d.take(n)
	if v == nil {
		return ""
	}
	if (!empty && n == 0) || !utf8.Valid(v) {
		d.err = errors.New("invalid text")
		return ""
	}
	for _, x := range v {
		if x == 0 {
			d.err = errors.New("NUL text")
		}
	}
	return string(v)
}
func (d *dec) bytes() []byte {
	n := d.u32()
	if uint64(n) > uint64(len(d.b)-d.i) {
		d.err = errors.New("bad length")
		return nil
	}
	return append([]byte(nil), d.take(int(n))...)
}
func (d *dec) caps() []uint16 {
	n := int(d.u16())
	if n > 64 {
		d.err = errors.New("too many capabilities")
		return nil
	}
	v := make([]uint16, n)
	var p uint16
	for i := range v {
		v[i] = d.u16()
		if v[i] == 0 || v[i] <= p {
			d.err = errors.New("bad capabilities")
		}
		p = v[i]
	}
	return v
}
func (d *dec) cl() ClientLimits { return ClientLimits{d.u32(), d.u32()} }
func (d *dec) lim() Limits {
	return Limits{d.u32(), d.u16(), d.u16(), d.u16(), d.u32(), d.u16(), d.u32(), d.u32(), d.u32(), d.u32()}
}
func (d *dec) key() StateKey { return StateKey{d.txt(false), d.txt(false)} }
func (d *dec) entry() StateEntry {
	return StateEntry{Namespace: d.txt(false), ObjectID: d.txt(false), Version: d.u64(), Deleted: d.boo(), Data: d.bytes()}
}
func Encode(f Frame) ([]byte, error) {
	if !f.Type.Valid() {
		return nil, errors.New("unknown frame")
	}
	e := &enc{}
	switch f.Type {
	case FrameHello:
		v, ok := f.Body.(Hello)
		if !ok {
			return nil, typ(f)
		}
		e.u8(v.Mode)
		e.txt(v.StreamID, v.Mode == 0)
		e.caps(v.Capabilities)
		e.cl(v.Limits)
		if v.Mode == 1 {
			e.u64(v.CursorLastSeq)
		}
		e.boo(v.StateSyncRequested)
	case FrameWelcome:
		v, ok := f.Body.(Welcome)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.SessionID)
		e.txt(v.StreamID, false)
		e.u8(v.Admission)
		e.u64(v.Generation)
		e.u64(v.ReplayTo)
		e.lim(v.Limits)
		e.caps(v.Capabilities)
	case FrameResume:
		v, ok := f.Body.(Resume)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.SessionID)
		e.txt(v.Cursor.StreamID, false)
		e.u64(v.Cursor.LastSeq)
		e.caps(v.Capabilities)
		e.cl(v.Limits)
		e.boo(v.StateSyncRequested)
	case FrameSend:
		v, ok := f.Body.(Send)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.MsgID)
		e.bytes(v.Content)
	case FrameAck:
		v, ok := f.Body.(Ack)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.MsgID)
		e.u8(v.Outcome)
		e.bytes(v.Result)
	case FrameQuickAck:
		v, ok := f.Body.(QuickAck)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.MsgID)
	case FrameRequest:
		v, ok := f.Body.(Request)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.RequestID)
		e.txt(v.Method, false)
		e.bytes(v.Params)
	case FrameResponse:
		v, ok := f.Body.(Response)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.RequestID)
		e.u8(v.Outcome)
		e.bytes(v.Result)
	case FrameEvent:
		v, ok := f.Body.(Event)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.EventID)
		e.u64(v.Seq)
		e.bytes(v.Content)
	case FrameRecoveryComplete:
		v, ok := f.Body.(RecoveryComplete)
		if !ok {
			return nil, typ(f)
		}
		e.u64(v.ReplayTo)
	case FrameStateQuery:
		v, ok := f.Body.(StateQuery)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.QueryID)
		if len(v.Keys) == 0 || len(v.Keys) > math.MaxUint16 {
			return nil, errors.New("bad key count")
		}
		e.u16(uint16(len(v.Keys)))
		for _, x := range v.Keys {
			e.key(x)
		}
	case FrameStateSnapshot:
		v, ok := f.Body.(StateSnapshot)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.QueryID)
		e.u32(v.PageIndex)
		e.boo(v.FinalPage)
		e.u16(uint16(len(v.Results)))
		for _, x := range v.Results {
			e.u8(uint8(x.Status))
			if x.Status != SnapshotMissing {
				e.entry(x.Entry)
			}
		}
	case FrameStateUpdate:
		v, ok := f.Body.(StateUpdate)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.UpdateID)
		e.entry(v.Entry)
	case FramePing:
		v, ok := f.Body.(Ping)
		if !ok {
			return nil, typ(f)
		}
		e.u64(v.Nonce)
		e.boo(v.Renew)
	case FramePong:
		v, ok := f.Body.(Pong)
		if !ok {
			return nil, typ(f)
		}
		e.u64(v.Nonce)
	case FrameError:
		v, ok := f.Body.(ErrorFrame)
		if !ok {
			return nil, typ(f)
		}
		e.u16(v.Code)
		e.boo(v.Retryable)
		e.u8(v.Ref.Kind)
		e.id(v.Ref.ID)
		e.u32(v.RetryAfterMS)
		e.txt(v.Detail, true)
	case FrameSyncBegin:
		v, ok := f.Body.(SyncBegin)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.SyncID)
		e.u64(v.BaselineSeq)
		e.txt(v.ScopeID, false)
		e.u32(v.TotalEntries)
	case FrameSyncPage:
		v, ok := f.Body.(SyncPage)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.SyncID)
		e.u32(v.PageIndex)
		e.boo(v.FinalPage)
		e.u16(uint16(len(v.Entries)))
		for _, x := range v.Entries {
			e.entry(x)
		}
	case FrameSyncCommit:
		v, ok := f.Body.(SyncCommit)
		if !ok {
			return nil, typ(f)
		}
		e.id(v.SyncID)
		e.u64(v.BaselineSeq)
	}
	if e.err != nil {
		return nil, e.err
	}
	if err := Validate(f); err != nil {
		return nil, err
	}
	if len(e.b)+8 > int(MaxFrameBytes) {
		return nil, errors.New("frame too large")
	}
	out := make([]byte, 8, len(e.b)+8)
	out[0] = 1
	out[1] = byte(f.Type)
	binary.BigEndian.PutUint32(out[4:], uint32(len(e.b)))
	return append(out, e.b...), nil
}
func typ(f Frame) error { return fmt.Errorf("wrong body for %d", f.Type) }
func Decode(raw []byte) (Frame, error) {
	if len(raw) < 8 {
		return Frame{}, errors.New("short header")
	}
	if raw[0] != 1 {
		return Frame{}, errors.New("unsupported version")
	}
	if raw[2] != 0 || raw[3] != 0 {
		return Frame{}, errors.New("flags")
	}
	if len(raw) > int(MaxFrameBytes) || uint64(binary.BigEndian.Uint32(raw[4:]))+8 != uint64(len(raw)) {
		return Frame{}, errors.New("length")
	}
	t := FrameType(raw[1])
	if !t.Valid() {
		return Frame{}, errors.New("unknown frame")
	}
	d := &dec{b: raw[8:]}
	f := Frame{Type: t}
	switch t {
	case FrameHello:
		v := Hello{Mode: d.u8()}
		v.StreamID = d.txt(v.Mode == 0)
		v.Capabilities = d.caps()
		v.Limits = d.cl()
		if v.Mode == 1 {
			v.CursorLastSeq = d.u64()
		}
		v.StateSyncRequested = d.boo()
		f.Body = v
	case FrameWelcome:
		f.Body = Welcome{d.id(), d.txt(false), d.u8(), d.u64(), d.u64(), d.lim(), d.caps()}
	case FrameResume:
		f.Body = Resume{d.id(), Cursor{d.txt(false), d.u64()}, d.caps(), d.cl(), d.boo()}
	case FrameSend:
		f.Body = Send{d.id(), d.bytes()}
	case FrameAck:
		f.Body = Ack{d.id(), d.u8(), d.bytes()}
	case FrameQuickAck:
		f.Body = QuickAck{d.id()}
	case FrameRequest:
		f.Body = Request{d.id(), d.txt(false), d.bytes()}
	case FrameResponse:
		f.Body = Response{d.id(), d.u8(), d.bytes()}
	case FrameEvent:
		f.Body = Event{d.id(), d.u64(), d.bytes()}
	case FrameRecoveryComplete:
		f.Body = RecoveryComplete{d.u64()}
	case FrameStateQuery:
		v := StateQuery{QueryID: d.id()}
		n := int(d.u16())
		v.Keys = make([]StateKey, n)
		for i := range v.Keys {
			v.Keys[i] = d.key()
		}
		f.Body = v
	case FrameStateSnapshot:
		v := StateSnapshot{QueryID: d.id(), PageIndex: d.u32(), FinalPage: d.boo()}
		n := int(d.u16())
		v.Results = make([]SnapshotResult, n)
		for i := range v.Results {
			v.Results[i].Status = SnapshotStatus(d.u8())
			if v.Results[i].Status != SnapshotMissing {
				v.Results[i].Entry = d.entry()
			}
		}
		f.Body = v
	case FrameStateUpdate:
		f.Body = StateUpdate{d.id(), d.entry()}
	case FramePing:
		f.Body = Ping{d.u64(), d.boo()}
	case FramePong:
		f.Body = Pong{d.u64()}
	case FrameError:
		f.Body = ErrorFrame{d.u16(), d.boo(), ErrorRef{d.u8(), d.id()}, d.u32(), d.txt(true)}
	case FrameSyncBegin:
		f.Body = SyncBegin{d.id(), d.u64(), d.txt(false), d.u32()}
	case FrameSyncPage:
		v := SyncPage{SyncID: d.id(), PageIndex: d.u32(), FinalPage: d.boo()}
		n := int(d.u16())
		v.Entries = make([]StateEntry, n)
		for i := range v.Entries {
			v.Entries[i] = d.entry()
		}
		f.Body = v
	case FrameSyncCommit:
		f.Body = SyncCommit{d.id(), d.u64()}
	}
	if d.err != nil {
		return Frame{}, d.err
	}
	if d.i != len(d.b) {
		return Frame{}, errors.New("trailing bytes")
	}
	if err := Validate(f); err != nil {
		return Frame{}, err
	}
	return f, nil
}
func Validate(f Frame) error {
	nz := func(id ID) bool { return !id.IsZero() }
	cl := func(v ClientLimits) bool {
		return v.MaxFrameBytes >= 4096 && v.MaxFrameBytes <= MaxFrameBytes && v.MaxSyncEntries > 0
	}
	switch v := f.Body.(type) {
	case Hello:
		if v.Mode > 1 || v.Mode == 0 && v.StreamID != "" || v.Mode == 1 && v.StreamID == "" || !cl(v.Limits) {
			return errors.New("invalid HELLO")
		}
	case Welcome:
		if !nz(v.SessionID) || v.Admission > 3 || v.Generation == 0 || validateLimits(v.Limits) != nil {
			return errors.New("invalid WELCOME")
		}
	case Resume:
		if !nz(v.SessionID) || !cl(v.Limits) {
			return errors.New("invalid RESUME")
		}
	case Send:
		if !nz(v.MsgID) {
			return errors.New("zero id")
		}
	case Ack:
		if !nz(v.MsgID) || v.Outcome > 1 {
			return errors.New("invalid ACK")
		}
	case QuickAck:
		if !nz(v.MsgID) {
			return errors.New("zero id")
		}
	case Request:
		if !nz(v.RequestID) || v.Method == "" || len(v.Method) > 255 {
			return errors.New("invalid REQUEST")
		}
	case Response:
		if !nz(v.RequestID) || v.Outcome > 1 {
			return errors.New("invalid RESPONSE")
		}
	case Event:
		if !nz(v.EventID) || v.Seq == 0 {
			return errors.New("invalid EVENT")
		}
	case StateQuery:
		if !nz(v.QueryID) || len(v.Keys) == 0 {
			return errors.New("invalid query")
		}
		m := map[StateKey]bool{}
		for _, x := range v.Keys {
			if m[x] {
				return errors.New("duplicate key")
			}
			m[x] = true
		}
	case StateSnapshot:
		if !nz(v.QueryID) {
			return errors.New("invalid snapshot")
		}
		for _, x := range v.Results {
			if x.Status > 2 {
				return errors.New("bad status")
			}
			if x.Status != 0 {
				if err := validateEntry(x.Entry); err != nil {
					return err
				}
			}
		}
	case StateUpdate:
		if !nz(v.UpdateID) {
			return errors.New("zero id")
		}
		return validateEntry(v.Entry)
	case ErrorFrame:
		if !knownError(v.Code) || v.Retryable != ErrorRetryable(v.Code) || v.Ref.Kind > 2 || v.Ref.Kind == 0 && !v.Ref.ID.IsZero() || v.Ref.Kind > 0 && v.Ref.ID.IsZero() || v.Code == ErrRateLimited && v.RetryAfterMS == 0 || v.Code != ErrRateLimited && v.Code != ErrSendPending && v.RetryAfterMS != 0 || len(v.Detail) > 1024 {
			return errors.New("invalid ERROR")
		}
	case SyncBegin:
		if !nz(v.SyncID) || v.ScopeID == "" {
			return errors.New("invalid sync")
		}
	case SyncPage:
		if !nz(v.SyncID) {
			return errors.New("invalid page")
		}
		for _, x := range v.Entries {
			if err := validateEntry(x); err != nil {
				return err
			}
		}
	case SyncCommit:
		if !nz(v.SyncID) {
			return errors.New("invalid commit")
		}
	}
	return nil
}
func validateEntry(v StateEntry) error {
	if v.Namespace == "" || v.ObjectID == "" || len(v.Namespace) > 255 || len(v.ObjectID) > 255 || v.Version == 0 || v.Deleted && len(v.Data) > 0 {
		return errors.New("invalid state")
	}
	return nil
}
func validateLimits(v Limits) error {
	if v.MaxFrameBytes < 4096 || v.MaxFrameBytes > MaxFrameBytes || v.MaxInflightSend == 0 || v.MaxInflightRequest == 0 || v.MaxInflightQuery == 0 || v.MaxReplayEvents == 0 || v.MaxStateItems == 0 || v.HeartbeatMS == 0 || v.IdleTimeoutMS <= v.HeartbeatMS || v.SessionTTLSeconds == 0 || v.AbsoluteRemainingSeconds == 0 || uint64(v.HeartbeatMS) > uint64(v.SessionTTLSeconds)*125 {
		return errors.New("invalid limits")
	}
	return nil
}
func knownError(v uint16) bool { return v >= 1 && v <= 17 || v == 1001 }
func NegotiateCapabilities(a, b []uint16) []uint16 {
	m := map[uint16]bool{}
	for _, x := range b {
		m[x] = true
	}
	o := []uint16{}
	for _, x := range a {
		if m[x] {
			o = append(o, x)
		}
	}
	sort.Slice(o, func(i, j int) bool { return o[i] < o[j] })
	return o
}
