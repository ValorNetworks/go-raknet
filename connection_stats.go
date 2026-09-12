package raknet

import (
	"sync"
	"time"
)

const (
	ConnectionSampleLimit = 64
	ConnectionStatsWindow = 10 * time.Second
)

// ConnectionStats is a copied, passive view of this connection. RTT samples
// are full send-to-ACK durations in ACK-processing order, limited to the last
// 64 clean transmissions within ten seconds. ACK batching influences timings.
// Resends are successful data transmissions, not detected loss or ACK packets.
type ConnectionStats struct {
	RTT                   [ConnectionSampleLimit]time.Duration
	Count                 int
	UpdatedAt             time.Time
	CapturedAt            time.Time
	OriginalTransmissions uint64
	Retransmissions       uint64
}

type rttObservation struct {
	at  time.Time
	rtt time.Duration
}

type transmissionBucket struct {
	second           int64
	original, resent uint64
}

type connectionStatistics struct {
	mu          sync.Mutex
	samples     [ConnectionSampleLimit]rttObservation
	next, count int
	updatedAt   time.Time
	epoch       time.Time
	buckets     [10]transmissionBucket
}

// ConnectionStats never takes the transport lock held during socket writes.
// It adds no traffic and returns a value with no aliases to internal storage.
func (conn *Conn) ConnectionStats() ConnectionStats {
	s := &conn.statistics
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(time.Now())
}

func (s *connectionStatistics) observeRTT(now time.Time, rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples[s.next] = rttObservation{now, rtt}
	s.next = (s.next + 1) % len(s.samples)
	s.count = min(s.count+1, len(s.samples))
	s.updatedAt = now
}

func (s *connectionStatistics) observeTransmission(now time.Time, resent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.epoch.IsZero() {
		s.epoch = now
	}
	second := int64(now.Sub(s.epoch) / time.Second)
	if second < 0 {
		return
	}
	bucket := &s.buckets[second%int64(len(s.buckets))]
	if bucket.second != second {
		*bucket = transmissionBucket{second: second}
	}
	if resent {
		bucket.resent++
	} else {
		bucket.original++
	}
}

func (s *connectionStatistics) snapshot(now time.Time) ConnectionStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked(now)
}

func (s *connectionStatistics) snapshotLocked(now time.Time) ConnectionStats {
	result := ConnectionStats{CapturedAt: now, UpdatedAt: s.updatedAt}
	for i := 0; i < s.count; i++ {
		observation := s.samples[(s.next-s.count+i+len(s.samples))%len(s.samples)]
		age := now.Sub(observation.at)
		if age < 0 || age >= ConnectionStatsWindow {
			continue
		}
		result.RTT[result.Count] = observation.rtt
		result.Count++
	}
	if !s.epoch.IsZero() {
		second := int64(now.Sub(s.epoch) / time.Second)
		for _, bucket := range s.buckets {
			if age := second - bucket.second; age >= 0 && age < int64(len(s.buckets)) {
				result.OriginalTransmissions += bucket.original
				result.Retransmissions += bucket.resent
			}
		}
	}
	return result
}
