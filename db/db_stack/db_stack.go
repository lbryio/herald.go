package db_stack

type SliceBackedStack struct {
	slice []interface{}
	len   uint32
}

func NewSliceBackedStack(size int) *SliceBackedStack {
	return &SliceBackedStack{
		slice: make([]interface{}, size),
		len:   0,
	}
}

func (s *SliceBackedStack) Push(v interface{}) {
	if s.len == uint32(len(s.slice)) {
		s.slice = append(s.slice, v)
	} else {
		s.slice[s.len] = v
	}
	s.len++
}

func (s *SliceBackedStack) Pop() interface{} {
	if s.len == 0 {
		return nil
	}
	s.len--
	return s.slice[s.len]
}

func (s *SliceBackedStack) Get(i uint32) interface{} {
	if i >= s.len {
		return nil
	}
	return s.slice[i]
}

func (s *SliceBackedStack) GetTip() interface{} {
	if s.len == 0 {
		return nil
	}
	return s.slice[s.len-1]
}

func (s *SliceBackedStack) Len() uint32 {
	return s.len
}

func (s *SliceBackedStack) Size() int {
	return len(s.slice)
}

func (s *SliceBackedStack) GetSlice() []interface{} {
	return s.slice
}
