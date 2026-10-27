package message

import (
	"sync"
	"testing"
)

func TestOpenConnectionRequest1ConcurrentMarshal(t *testing.T) {
	// Each dial can marshal a handshake using a previously unseen MTU.
	cachedOCR1Mu.Lock()
	previous := cachedOCR1
	cachedOCR1 = make(map[uint16][]byte)
	cachedOCR1Mu.Unlock()
	t.Cleanup(func() {
		cachedOCR1Mu.Lock()
		cachedOCR1 = previous
		cachedOCR1Mu.Unlock()
	})
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			<-start
			for i := 0; i < 16; i++ {
				pk := OpenConnectionRequest1{ClientProtocol: 11, MTU: uint16(576 + worker*16 + i)}
				data, err := pk.MarshalBinary()
				if err != nil || len(data) != int(pk.MTU)-28 || data[17] != pk.ClientProtocol {
					t.Errorf("invalid handshake for MTU %d: length=%d err=%v", pk.MTU, len(data), err)
					return
				}
			}
		}(worker)
	}
	close(start)
	workers.Wait()
}
