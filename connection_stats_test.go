package raknet

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/sandertv/go-raknet/congestion"
)

func TestStatisticsRingExpiryAndCopy(t *testing.T) {
	var stats connectionStatistics
	now := time.Now()
	stats.observeRTT(now, 0)
	stats.observeRTT(now, -time.Second)
	if stats.snapshot(now).Count != 0 {
		t.Fatal("invalid samples included")
	}
	for i := 1; i <= 80; i++ {
		stats.observeRTT(now, time.Duration(i)*time.Microsecond)
	}
	got := stats.snapshot(now)
	if got.Count != 64 || got.RTT[0] != 17*time.Microsecond || got.RTT[63] != 80*time.Microsecond {
		t.Fatalf("bad bounded ring: %+v", got)
	}
	got.RTT[0] = 0
	if stats.snapshot(now).RTT[0] == 0 {
		t.Fatal("snapshot aliases ring")
	}
	if stats.snapshot(now.Add(10*time.Second)).Count != 0 {
		t.Fatal("stale samples included")
	}
	stats.observeRTT(now.Add(11*time.Second), time.Second)
	got = stats.snapshot(now.Add(11 * time.Second))
	if got.Count != 1 || got.RTT[0] != time.Second {
		t.Fatal("idle gap retained expired history")
	}
}

func TestStatisticsTransmissionBuckets(t *testing.T) {
	var stats connectionStatistics
	now := time.Now()
	if stats.snapshot(now).OriginalTransmissions != 0 {
		t.Fatal("invented traffic")
	}
	stats.observeTransmission(now, false)
	stats.observeTransmission(now, true)
	stats.observeTransmission(now.Add(9*time.Second), true)
	got := stats.snapshot(now.Add(9 * time.Second))
	if got.OriginalTransmissions != 1 || got.Retransmissions != 2 {
		t.Fatalf("counts = %+v", got)
	}
	got = stats.snapshot(now.Add(10 * time.Second))
	if got.OriginalTransmissions != 0 || got.Retransmissions != 1 {
		t.Fatalf("rollover = %+v", got)
	}
	stats.observeTransmission(now.Add(50*time.Second), false)
	got = stats.snapshot(now.Add(50 * time.Second))
	if got.OriginalTransmissions != 1 || got.Retransmissions != 0 {
		t.Fatalf("idle rollover = %+v", got)
	}
}

func TestRecoveryCleanSamplesAndDuplicates(t *testing.T) {
	recovery := newRecoveryQueue()
	for _, eligible := range []bool{true, false} {
		recovery.add(1, nil, 100, eligible)
		record := recovery.unacknowledged[1]
		record.timestamp = time.Now().Add(-250 * time.Microsecond)
		recovery.unacknowledged[1] = record
		_, sample, ok := recovery.acknowledge(1)
		if !ok || (sample > 0) != eligible {
			t.Fatalf("eligible=%v sample=%v ok=%v", eligible, sample, ok)
		}
		if sample > 0 && sample != recovery.lastSample {
			t.Fatal("diagnostic measured RTT a second time")
		}
		if _, sample, ok := recovery.acknowledge(1); ok || sample != 0 {
			t.Fatal("duplicate ACK produced sample")
		}
		if recovery.inFlightBytesEstimate() != 0 {
			t.Fatal("recovery bookkeeping changed")
		}
	}
	if recovery.srtt == 0 {
		t.Fatal("existing estimator disabled")
	}
}

func statsTestConn(write func([]byte, net.Addr) (int, error)) *Conn {
	// No live ticker: each test drives sends and ACKs explicitly.
	return &Conn{
		conn: &mockPacketConn{writeToFunc: write}, raddr: &net.UDPAddr{}, mtu: 1400,
		handler: &mockListenerHandler{logger: slog.New(slog.NewTextHandler(io.Discard, nil))},
		buf:     bytes.NewBuffer(make([]byte, 0, 1400)), retransmission: newRecoveryQueue(),
		cc: congestion.NewUDT(1400), ccNackSet: make(map[uint24]struct{}),
	}
}

func TestStatisticsAcceptReorderedACKsOnce(t *testing.T) {
	conn := statsTestConn(nil)
	for range 2 {
		_, _ = conn.sendDatagram(nil)
	}
	for seq, age := range map[uint24]time.Duration{0: time.Second, 1: time.Millisecond} {
		record := conn.retransmission.unacknowledged[seq]
		record.timestamp = time.Now().Add(-age)
		conn.retransmission.unacknowledged[seq] = record
	}
	for _, seq := range []uint24{1, 0, 1, 999} {
		var payload bytes.Buffer
		ack := acknowledgement{packets: []uint24{seq}}
		ack.write(&payload, 1400)
		if err := conn.handleACK(payload.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
	got := conn.ConnectionStats()
	if got.Count != 2 || got.RTT[0] >= got.RTT[1] {
		t.Fatalf("duplicate/unknown ACK counted or reordered ACK dropped: %+v", got)
	}
}

func TestStatisticsCountsOnlySuccessfulDataWrites(t *testing.T) {
	for _, test := range []struct {
		name    string
		write   func([]byte, net.Addr) (int, error)
		success bool
		fatal   bool
	}{
		{"success", func(p []byte, _ net.Addr) (int, error) { return len(p), nil }, true, false},
		{"recoverable error", func([]byte, net.Addr) (int, error) { return 0, errors.New("temporary") }, false, false},
		{"closed", func([]byte, net.Addr) (int, error) { return 0, net.ErrClosed }, false, true},
		{"short", func([]byte, net.Addr) (int, error) { return 0, nil }, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := statsTestConn(test.write)
			if _, err := conn.sendDatagram(nil); (err != nil) != test.fatal {
				t.Fatalf("error semantics changed: %v", err)
			}
			_, _ = conn.sendDatagramKind(nil, true)
			_, _ = conn.sendDatagramKind(nil, true)
			_ = conn.sendUnreliableDatagram(nil)
			_ = conn.writeTo([]byte{0}, conn.raddr) // Control writes aren't counted.
			got := conn.ConnectionStats()
			want := uint64(0)
			if test.success {
				want = 2
			}
			if got.OriginalTransmissions != want || got.Retransmissions != want {
				t.Fatalf("counts = %+v", got)
			}
			for _, record := range conn.retransmission.unacknowledged {
				if !test.success && record.sampleEligible {
					t.Fatal("failed write can generate sample")
				}
			}
		})
	}
}

func TestStatisticsResendLineage(t *testing.T) {
	conn := statsTestConn(nil)
	_, _ = conn.sendDatagram(nil)
	for round := 0; round < 3; round++ {
		seq := conn.seq - 1
		record := conn.retransmission.unacknowledged[seq]
		if record.sampleEligible != (round == 0) {
			t.Fatalf("round %d: eligibility=%v", round, record.sampleEligible)
		}
		conn.enqueueResendsLocked([]uint24{seq})
		conn.resendBudget = 10000
		conn.flushResendQueueLocked()
	}
	if _, sample, ok := conn.retransmission.acknowledge(conn.seq - 1); !ok || sample != 0 {
		t.Fatal("resent ACK generated clean sample")
	}
	got := conn.ConnectionStats()
	if got.OriginalTransmissions != 1 || got.Retransmissions != 3 {
		t.Fatalf("resend counts=%+v", got)
	}
}

func TestStatisticsReadersDoNotWaitForTransportLock(t *testing.T) {
	conn := statsTestConn(nil)
	conn.mu.Lock()
	defer conn.mu.Unlock()
	done := make(chan struct{})
	go func() { _ = conn.ConnectionStats(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("snapshot waits on transport mutex")
	}
}

func TestStatisticsConcurrentReadersAndWriters(t *testing.T) {
	var stats connectionStatistics
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Go(func() {
			for i := 0; i < 1000; i++ {
				now := time.Now()
				stats.observeRTT(now, time.Duration(i+1))
				stats.observeTransmission(now, i%2 == 0)
				got := stats.snapshot(now)
				if got.Count > 64 {
					t.Error("unbounded snapshot")
				}
			}
		})
	}
	wg.Wait()
}

func BenchmarkStatisticsObservation(b *testing.B) {
	var stats connectionStatistics
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		stats.observeRTT(now, 50*time.Millisecond)
		stats.observeTransmission(now, false)
	}
}

func BenchmarkStatisticsSnapshot(b *testing.B) {
	var stats connectionStatistics
	now := time.Now()
	for i := 0; i < 64; i++ {
		stats.observeRTT(now, 50*time.Millisecond)
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = stats.snapshot(now)
	}
}
