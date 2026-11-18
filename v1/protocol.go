package kmtproto

import "time"

const (
	WireVersionV1      uint8  = 1
	MaxFrameBytes      uint32 = 1 << 20
	CapabilityQuickACK uint16 = 1
)

type FrameType uint8

const (
	FrameHello FrameType = iota + 1
	FrameWelcome
	FrameResume
	FrameSend
	FrameAck
	FrameQuickAck
	FrameRequest
	FrameResponse
	FrameEvent
	FrameRecoveryComplete
	FrameStateQuery
	FrameStateSnapshot
	FrameStateUpdate
	FramePing
	FramePong
	FrameError
	FrameSyncBegin
	FrameSyncPage
	FrameSyncCommit
)

func (t FrameType) Valid() bool { return t >= FrameHello && t <= FrameSyncCommit }

type ID [16]byte

func (id ID) IsZero() bool { return id == ID{} }

type Frame struct {
	Type FrameType
	Body any
}
type Cursor struct {
	StreamID string
	LastSeq  uint64
}
type ClientLimits struct{ MaxFrameBytes, MaxSyncEntries uint32 }
type Limits struct {
	MaxFrameBytes                                                           uint32
	MaxInflightSend, MaxInflightRequest, MaxInflightQuery                   uint16
	MaxReplayEvents                                                         uint32
	MaxStateItems                                                           uint16
	HeartbeatMS, IdleTimeoutMS, SessionTTLSeconds, AbsoluteRemainingSeconds uint32
}
type StateKey struct{ Namespace, ObjectID string }
type StateEntry struct {
	Namespace, ObjectID string
	Version             uint64
	Deleted             bool
	Data                []byte
}
type SnapshotStatus uint8

const (
	SnapshotMissing SnapshotStatus = iota
	SnapshotPresent
	SnapshotDeleted
)

type SnapshotResult struct {
	Status SnapshotStatus
	Entry  StateEntry
}
type ErrorRef struct {
	Kind uint8
	ID   ID
}
type Hello struct {
	Mode               uint8
	StreamID           string
	Capabilities       []uint16
	Limits             ClientLimits
	CursorLastSeq      uint64
	StateSyncRequested bool
}
type Welcome struct {
	SessionID            ID
	StreamID             string
	Admission            uint8
	Generation, ReplayTo uint64
	Limits               Limits
	Capabilities         []uint16
}
type Resume struct {
	SessionID          ID
	Cursor             Cursor
	Capabilities       []uint16
	Limits             ClientLimits
	StateSyncRequested bool
}
type Send struct {
	MsgID   ID
	Content []byte
}
type Ack struct {
	MsgID   ID
	Outcome uint8
	Result  []byte
}
type QuickAck struct{ MsgID ID }
type Request struct {
	RequestID ID
	Method    string
	Params    []byte
}
type Response struct {
	RequestID ID
	Outcome   uint8
	Result    []byte
}
type Event struct {
	EventID ID
	Seq     uint64
	Content []byte
}
type RecoveryComplete struct{ ReplayTo uint64 }
type StateQuery struct {
	QueryID ID
	Keys    []StateKey
}
type StateSnapshot struct {
	QueryID   ID
	PageIndex uint32
	FinalPage bool
	Results   []SnapshotResult
}
type StateUpdate struct {
	UpdateID ID
	Entry    StateEntry
}
type Ping struct {
	Nonce uint64
	Renew bool
}
type Pong struct{ Nonce uint64 }
type ErrorFrame struct {
	Code         uint16
	Retryable    bool
	Ref          ErrorRef
	RetryAfterMS uint32
	Detail       string
}
type SyncBegin struct {
	SyncID       ID
	BaselineSeq  uint64
	ScopeID      string
	TotalEntries uint32
}
type SyncPage struct {
	SyncID    ID
	PageIndex uint32
	FinalPage bool
	Entries   []StateEntry
}
type SyncCommit struct {
	SyncID      ID
	BaselineSeq uint64
}

const (
	AdmissionNew uint8 = iota
	AdmissionAttached
	AdmissionResumed
	AdmissionFullSync
)
const (
	OutcomeSuccess uint8 = iota
	OutcomeApplicationError
)
const (
	ErrMalformedFrame uint16 = 1 + iota
	ErrProtocolState
	ErrUnauthorized
	ErrAdmissionRejected
	ErrLimitExceeded
	ErrUnsupportedCapability
	ErrStateConflict
	ErrStateQueryFailed
	ErrSendConflict
	ErrSendPending
	ErrSessionExpired
	ErrSyncAborted
	ErrReplayPartial
	ErrRateLimited
	ErrAuthRevoked
	ErrUnsupportedVersion
	ErrReplayUnavailable
)
const ErrSendIndeterminate uint16 = 1001

func ErrorRetryable(c uint16) bool {
	return c == ErrStateQueryFailed || c == ErrSendPending || c == ErrReplayPartial || c == ErrRateLimited
}
func DefaultLimits() Limits {
	return Limits{MaxFrameBytes: MaxFrameBytes, MaxInflightSend: 64, MaxInflightRequest: 64, MaxInflightQuery: 16, MaxReplayEvents: 1024, MaxStateItems: 128, HeartbeatMS: 10000, IdleTimeoutMS: 30000, SessionTTLSeconds: 300, AbsoluteRemainingSeconds: 3600}
}
func (l Limits) RenewalInterval() time.Duration {
	return time.Duration(l.SessionTTLSeconds) * 250 * time.Millisecond
}
