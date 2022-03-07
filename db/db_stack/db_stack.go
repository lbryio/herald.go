package db_stack

import "sync"

type SliceBackedStack struct {
	slice []interface{}
	len   uint32
	mut   sync.RWMutex
}

func NewSliceBackedStack(size int) *SliceBackedStack {
	return &SliceBackedStack{
		slice: make([]interface{}, size),
		len:   0,
		mut:   sync.RWMutex{},
	}
}

func (s *SliceBackedStack) Push(v interface{}) {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.len == uint32(len(s.slice)) {
		s.slice = append(s.slice, v)
	} else {
		s.slice[s.len] = v
	}
	s.len++
}

func (s *SliceBackedStack) Pop() interface{} {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.len == 0 {
		return nil
	}
	s.len--
	return s.slice[s.len]
}

func (s *SliceBackedStack) Get(i uint32) interface{} {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if i >= s.len {
		return nil
	}
	return s.slice[i]
}

func (s *SliceBackedStack) GetTip() interface{} {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.len == 0 {
		return nil
	}
	return s.slice[s.len-1]
}

func (s *SliceBackedStack) Len() uint32 {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return s.len
}

func (s *SliceBackedStack) Cap() int {
	s.mut.RLock()
	defer s.mut.RUnlock()

	return cap(s.slice)
}

func (s *SliceBackedStack) GetSlice() []interface{} {
	// This is not thread safe so I won't bother with locking
	return s.slice
}
