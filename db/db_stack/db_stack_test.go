package db_stack_test

import (
	"testing"
	"time"

	"github.com/lbryio/hub/db/db_stack"
)

func TestPush(t *testing.T) {
	var want uint32 = 3

	stack := db_stack.NewSliceBackedStack(10)

	stack.Push(0)
	stack.Push(1)
	stack.Push(2)

	if got := stack.Len(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPushPop(t *testing.T) {
	stack := db_stack.NewSliceBackedStack(10)

	for i := 0; i < 5; i++ {
		stack.Push(i)
	}
	for i := 0; i < 5; i++ {
		wantLen := 5 - i

		if got := stack.Len(); int(got) != wantLen {
			t.Errorf("got %v, want %v", got, wantLen)
		}

		if got := stack.Pop(); got != 5-i-1 {
			t.Errorf("got %v, want %v", got, 5-i-1)
		}

		wantLen -= 1

		if got := stack.Len(); int(got) != wantLen {
			t.Errorf("got %v, want %v", got, wantLen)
		}
	}
}

func doPushes(stack *db_stack.SliceBackedStack, numPushes int) {
	for i := 0; i < numPushes; i++ {
		stack.Push(i)
	}
}

func doPops(stack *db_stack.SliceBackedStack, numPops int) {
	for i := 0; i < numPops; i++ {
		stack.Pop()
	}
}

func TestMultiThreaded(t *testing.T) {
	stack := db_stack.NewSliceBackedStack(100000)

	go doPushes(stack, 100000)
	go doPushes(stack, 100000)
	go doPushes(stack, 100000)

	time.Sleep(time.Second)

	if stack.Len() != 300000 {
		t.Errorf("got %v, want %v", stack.Len(), 300000)
	}

	go doPops(stack, 100000)
	go doPops(stack, 100000)
	go doPops(stack, 100000)

	time.Sleep(time.Second)

	if stack.Len() != 0 {
		t.Errorf("got %v, want %v", stack.Len(), 0)
	}
}

func TestGet(t *testing.T) {
	stack := db_stack.NewSliceBackedStack(10)

	for i := 0; i < 5; i++ {
		stack.Push(i)
	}

	if got := stack.GetTip(); got != 4 {
		t.Errorf("got %v, want %v", got, 4)
	}

	for i := 0; i < 5; i++ {
		if got := stack.Get(uint32(i)); got != i {
			t.Errorf("got %v, want %v", got, i)
		}
	}

	slice := stack.GetSlice()

	if len(slice) != 10 {
		t.Errorf("got %v, want %v", len(slice), 10)
	}
}

func TestLenCap(t *testing.T) {
	stack := db_stack.NewSliceBackedStack(10)

	if got := stack.Len(); got != 0 {
		t.Errorf("got %v, want %v", got, 0)
	}

	if got := stack.Cap(); got != 10 {
		t.Errorf("got %v, want %v", got, 10)
	}
}
