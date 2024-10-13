// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"fmt"
	"math/big"
)

const (
	free      = 0
	allocated = 1
)

// allocator maintains a bitset of allocated numbers.
// 分配器维护一组已分配的数字。
// 维护一个big.Int类型的变量pool作为位集，用于表示给定范围内整数的分配状态。
// 每个位对应一个整数，0 表示未分配（free），1 表示已分配（allocated）
type allocator struct {
	pool   *big.Int
	follow int
	low    int
	high   int
}

// NewAllocator reserves and frees integers out of a range between low and
// high.
//
// O(N) worst case space used, where N is maximum allocated, divided by
// sizeof(big.Word)
func newAllocator(low, high int) *allocator {
	return &allocator{
		pool:   big.NewInt(0),
		follow: low,
		low:    low,
		high:   high,
	}
}

// String returns a string describing the contents of the allocator like
// "allocator[low..high] reserved..until"
//
// O(N) where N is high-low
func (a allocator) String() string {
	b := &bytes.Buffer{}
	fmt.Fprintf(b, "allocator[%d..%d]", a.low, a.high)

	for low := a.low; low <= a.high; low++ {
		high := low
		for a.reserved(high) && high <= a.high {
			high++
		}

		if high > low+1 {
			fmt.Fprintf(b, " %d..%d", low, high-1)
		} else if high > low {
			fmt.Fprintf(b, " %d", high-1)
		}

		low = high
	}
	return b.String()
}

// Next reserves and returns the next available number out of the range between
// low and high.  If no number is available, false is returned.
// 下一个储存并返回 低至高范围内的 下一个可用数字。如果没有可用数字，则返回虚假数字。
//
// O(N) worst case runtime where N is allocated, but usually O(1) due to a
// rolling index into the oldest allocation.
// 0(N)运行时最坏的情况，其中分配了N，但通常是0(1)，原因是滚动索引进入最旧的分配。
// 提供了next方法用于分配下一个可用的整数。该方法从follow指向的位置开始查找，
// 采用环形查找策略，如果没有找到可用数字，则返回false。
func (a *allocator) next() (int, bool) {
	wrapped := a.follow
	defer func() {
		// make a.follow point to next value
		if a.follow == a.high { // 由low到high的数字环形
			a.follow = a.low
		} else {
			a.follow += 1
		}
	}()

	// Find trailing bit
	for ; a.follow <= a.high; a.follow++ {
		if a.reserve(a.follow) {
			return a.follow, true
		}
	}

	// Find preceding free'd pool
	a.follow = a.low

	for ; a.follow < wrapped; a.follow++ {
		if a.reserve(a.follow) {
			return a.follow, true
		}
	}

	return 0, false
}

// reserve claims the bit if it is not already claimed, returning true if
// successfully claimed.
// reserve方法用于尝试分配一个特定的整数，如果该整数未被分配，则将其标记为已分配并返回true，
// 否则返回false。
func (a *allocator) reserve(n int) bool {
	if a.reserved(n) {
		return false
	}
	a.pool.SetBit(a.pool, n-a.low, allocated)
	return true
}

// reserved returns true if the integer has been allocated
// reserved方法用于检查一个整数是否已被分配。
func (a *allocator) reserved(n int) bool {
	return a.pool.Bit(n-a.low) == allocated
}

// release frees the use of the number for another allocation
// release方法用于释放一个已分配的整数，将其对应的位设置为未分配状态
func (a *allocator) release(n int) {
	a.pool.SetBit(a.pool, n-a.low, free)
}
