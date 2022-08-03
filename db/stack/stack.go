package stack

// The db_stack package contains the implementation of a generic slice backed stack
// used for tracking various states in the hub, i.e. headers and txcounts

import (
	"sync"

	"github.com/lbryio/herald.go/internal"
)

type SliceBacked struct {
	slice []interface{}
	len   uint32
	mut   sync.RWMutex
}

func NewSliceBacked(size int) *SliceBacked {
	return &SliceBacked{
		slice: make([]interface{}, size),
		len:   0,
		mut:   sync.RWMutex{},
	}
}

func (s *SliceBacked) Push(v interface{}) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.len == uint32(len(s.slice)) {
		s.slice = append(s.slice, v)
	} else {
		s.slice[s.len] = v
	}
	s.len++
}

func (s *SliceBacked) Pop() interface{} {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.len == 0 {
		return nil
	}
	s.len--
	return s.slice[s.len]
}

func (s *SliceBacked) Get(i uint32) interface{} {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if i >= s.len {
		return nil
	}
	return s.slice[i]
}

func (s *SliceBacked) GetTip() interface{} {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.len == 0 {
		return nil
	}
	return s.slice[s.len-1]
}

func (s *SliceBacked) Len() uint32 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.len
}

func (s *SliceBacked) Cap() int {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return cap(s.slice)
}

func (s *SliceBacked) GetSlice() []interface{} {
	// This is not thread safe so I won't bother with locking
	return s.slice
}

// This function is dangerous because it assumes underlying types
func (s *SliceBacked) TxCountsBisectRight(txNum, rootTxNum uint32) (uint32, uint32) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	txCounts := s.slice[:s.Len()]
	height := internal.BisectRight(txCounts, txNum)
	createdHeight := internal.BisectRight(txCounts, rootTxNum)

	return height, createdHeight
}
