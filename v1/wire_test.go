package kmtproto

import (
	"context"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestGoldenVectors(t *testing.T) {
	v := []string{
		"010100000000000e0000000000001000000001000000", "01020000000000460000000000000000000000000000000100017300000000000000000100000000000000000000100000020002000200000064000a000003e800000bb80000003c00000e100000", "01030000000000260000000000000000000000000000000100017300000000000000000000001000000001000000", "0104000000000015000000000000000000000000000000020000000141", "0105000000000015000000000000000000000000000000020000000000", "010600000000001000000000000000000000000000000002", "01070000000000170000000000000000000000000000000300017100000000", "0108000000000015000000000000000000000000000000030000000000", "010900000000001d0000000000000000000000000000000400000000000000010000000141", "010a0000000000080000000000000001", "010b00000000001800000000000000000000000000000005000100016e00016b", "010c00000000002c00000000000000000000000000000005000000000100010100016e00016b0000000000000001000000000141", "010d0000000000240000000000000000000000000000000600016e00016b0000000000000001000000000141", "010e000000000009000000000000000700", "010f0000000000080000000000000007", "011000000000001a0002000000000000000000000000000000000000000000000000", "011100000000001f00000000000000000000000000000007000000000000000100017300000001", "011200000000002b000000000000000000000000000000070000000001000100016e00016b0000000000000001000000000141", "0113000000000018000000000000000000000000000000070000000000000001"}
	for i, s := range v {
		b, _ := hex.DecodeString(s)
		f, e := Decode(b)
		if e != nil {
			t.Fatalf("%d decode %v", i, e)
		}
		g, e := Encode(f)
		if e != nil || hex.EncodeToString(g) != s {
			t.Fatalf("%d roundtrip %v %x", i, e, g)
		}
	}
}
func TestMalformed(t *testing.T) {
	b, _ := hex.DecodeString("010e000000000009000000000000000700")
	cases := [][]byte{b[:len(b)-1], append(append([]byte{}, b...), 0)}
	x := append([]byte{}, b...)
	x[3] = 1
	cases = append(cases, x)
	x = append([]byte{}, b...)
	x[len(x)-1] = 2
	cases = append(cases, x)
	for i, x := range cases {
		if _, e := Decode(x); e == nil {
			t.Fatalf("%d accepted", i)
		}
	}
}
func now() time.Time { return time.Unix(1700000000, 0) }
func TestConcurrentClaimAndFencing(t *testing.T) {
	m := NewMemoryStore()
	id := ID{15: 1}
	n := now()
	e := m.CreateSession(context.Background(), Session{ID: id, Principal: "p", StreamID: "s", Generation: 1, ExpiresAt: n.Add(time.Hour), AbsoluteExpiresAt: n.Add(2 * time.Hour)})
	if e != nil {
		t.Fatal(e)
	}
	msg := ID{15: 2}
	var fresh atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, f, e := m.ClaimSend(context.Background(), id, 1, Send{msg, []byte("x")})
			if e != nil {
				t.Error(e)
			}
			if f {
				fresh.Add(1)
			}
		}()
	}
	wg.Wait()
	if fresh.Load() != 1 {
		t.Fatal(fresh.Load())
	}
	s, _, e := m.AdmitResume(context.Background(), id, "p", "s", 0, n.Add(time.Second), time.Hour)
	if e != nil || s.Generation != 2 {
		t.Fatal(e, s.Generation)
	}
	if _, _, e = m.ClaimSend(context.Background(), id, 1, Send{ID{15: 3}, nil}); e != ErrFenced {
		t.Fatal(e)
	}
}
func TestStateRawConflict(t *testing.T) {
	m := NewMemoryStore()
	id := ID{15: 1}
	n := now()
	_ = m.CreateSession(context.Background(), Session{ID: id, Principal: "p", StreamID: "s", Generation: 1, ExpiresAt: n.Add(time.Hour), AbsoluteExpiresAt: n.Add(2 * time.Hour)})
	v := StateEntry{Namespace: "n", ObjectID: "o", Version: 1, Data: []byte("{\"a\":1}")}
	if e := m.PutState(context.Background(), "s", v); e != nil {
		t.Fatal(e)
	}
	v.Data = []byte("{ \"a\": 1 }")
	if e := m.PutState(context.Background(), "s", v); e == nil {
		t.Fatal("conflict accepted")
	}
}
