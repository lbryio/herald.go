package internal

import "sort"

// BisectRight returns the index of the first element in the list that is greater than or equal to the value.
// https://stackoverflow.com/questions/29959506/is-there-a-go-analog-of-pythons-bisect-module
func BisectRight(arr []interface{}, val uint32) uint32 {
	i := sort.Search(len(arr), func(i int) bool { return arr[i].(uint32) >= val })
	return uint32(i)
}
