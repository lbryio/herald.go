package stack

// The db_stack package contains the implementation of a generic slice backed stack
// used for tracking various states in the hub, i.e. headers and txcounts

import (
	"sync"

	"github.com/lbryio/herald.go/internal"
	"golang.org/x/exp/constraints"
)

type SliceBacked[T any] struct {
	slice []T
	len   uint32
	mut   sync.RWMutex
}

func NewSliceBacked[T any](size int) *SliceBacked[T] {
	return &SliceBacked[T]{
		slice: make([]T, size),
		len:   0,
		mut:   sync.RWMutex{},
	}
}

func (s *SliceBacked[T]) Push(v T) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.len == uint32(len(s.slice)) {
		s.slice = append(s.slice, v)
	} else {
		s.slice[s.len] = v
	}
	s.len++
}

func (s *SliceBacked[T]) Pop() T {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.len == 0 {
		var null T
		return null
	}
	s.len--
	return s.slice[s.len]
}

func (s *SliceBacked[T]) Get(i uint32) T {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if i >= s.len {
		var null T
		return null
	}
	return s.slice[i]
}

func (s *SliceBacked[T]) GetTip() T {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.len == 0 {
		var null T
		return null
	}
	return s.slice[s.len-1]
}

func (s *SliceBacked[T]) Len() uint32 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.len
}

func (s *SliceBacked[T]) Cap() int {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return cap(s.slice)
}

func (s *SliceBacked[T]) GetSlice() []T {
	// This is not thread safe so I won't bother with locking
	return s.slice
}

func BisectRight[T constraints.Ordered](s *SliceBacked[T], searchKeys []T) []uint32 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	found := make([]uint32, len(searchKeys))
	for i, k := range searchKeys {
		found[i] = internal.BisectRight(s.slice[:s.Len()], k)
	}

	return found
}
