package internal

import (
	"sort"

	"golang.org/x/exp/constraints"
)

// BisectRight returns the index of the first element in the list that is greater than or equal to the value.
// https://stackoverflow.com/questions/29959506/is-there-a-go-analog-of-pythons-bisect-module
func BisectRight[T constraints.Ordered](arr []T, val T) uint32 {
	i := sort.Search(len(arr), func(i int) bool { return arr[i] >= val })
	return uint32(i)
}
