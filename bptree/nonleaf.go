package bptree

import (
	"encoding/binary"

	"github.com/roy2220/fsm"
)

const (
	nonLeafSize               = 1 << 13
	maxNonLeafFreeSpaceSize   = nonLeafSize - nonLeafHeaderSize
	maxNonLeafChildSize       = nonLeafChildHeaderSize + maxKeySize
	nonLeafOverloadThreshold  = maxNonLeafFreeSpaceSize - maxNonLeafChildSize
	nonLeafUnderloadThreshold = (nonLeafOverloadThreshold-maxNonLeafChildSize)/2 + 1
)

type nonLeafFactory struct {
	FileStorage *fsm.FileStorage
}

func (nlf nonLeafFactory) CreateNonLeaf() (int64, nonLeafController) {
	nonLeafAddr, nonLeafAccessor := nlf.FileStorage.AllocateAlignedSpace(nonLeafSize)

	for i := 0; i < nonLeafHeaderSize; i++ {
		nonLeafAccessor[i] = 0
	}

	return nonLeafAddr, nonLeafController(nonLeafAccessor)
}

func (nlf nonLeafFactory) DestroyNonLeaf(nonLeafAddr int64) {
	nlf.FileStorage.FreeAlignedSpace(nonLeafAddr)
}

func (nlf nonLeafFactory) GetNonLeafController(nonLeafAddr int64) nonLeafController {
	return nonLeafController(nlf.FileStorage.AccessAlignedSpace(nonLeafAddr))
}

type nonLeafController []byte

func (nlc nonLeafController) LocateChild(key []byte, keyComparer keyComparer) (int, bool) {
	if isMinKey(key) {
		return 0, true
	}

	n := nlc.NumberOfChildren()

	if isMaxKey(key) {
		return n - 1, true
	}

	i, j := 1 /* skip the first child whose key is dummy */, n-1

	for i < j {
		k := (i + j) / 2
		// i <= k < j

		if keyComparer.CompareKey(nlc.GetKey(k), key) < 0 {
			i = k + 1
			// i <= j
		} else {
			j = k
			// j >= i
		}
	}
	// i == j

	d := keyComparer.CompareKey(nlc.GetKey(i), key)

	if d == 0 {
		return i, true
	}

	if d < 0 && i == n-1 {
		i = n
	}

	return i, false
}

func (nlc nonLeafController) InsertChildren(firstChildIndex int, children []nonLeafChild) {
	numberOfChildrenX := nlc.checkChildIndex(firstChildIndex)
	childHeadersOffset := nonLeafHeaderSize + firstChildIndex*nonLeafChildHeaderSize
	childHeadersEndOffsetX := nonLeafHeaderSize + numberOfChildrenX*nonLeafChildHeaderSize
	childHeadersSize := len(children) * nonLeafChildHeaderSize
	var keysOffsetX int

	if numberOfChildrenX == 0 {
		keysOffsetX = nonLeafSize
	} else {
		keysOffsetX = int(nonLeafChildHeader(nlc[nonLeafHeaderSize:]).KeyOffset())
	}

	var keysEndOffset int

	if childHeadersOffset == childHeadersEndOffsetX {
		keysEndOffset = nonLeafSize
	} else {
		keysEndOffset = int(nonLeafChildHeader(nlc[childHeadersOffset:]).KeyOffset())
	}

	keysSize := 0

	for _, child := range children {
		keysSize += len(child.Key)
	}

	nlc.doInsertChildren(
		childHeadersOffset,
		childHeadersEndOffsetX,
		childHeadersSize,
		keysOffsetX,
		keysEndOffset,
		keysSize,
		children,
	)

	nonLeafHeader(nlc).SetChildCount(int32(numberOfChildrenX + len(children)))
}

func (nlc nonLeafController) RemoveChildren(firstChildIndex int, numberOfChildren int) []nonLeafChild {
	numberOfChildrenX := nlc.checkChildIndex(firstChildIndex)

	if firstChildIndex+numberOfChildren > numberOfChildrenX {
		panic(errOutOfRange)
	}

	childHeadersOffset := nonLeafHeaderSize + firstChildIndex*nonLeafChildHeaderSize
	childHeadersSize := numberOfChildren * nonLeafChildHeaderSize
	childHeadersEndOffset := childHeadersOffset + childHeadersSize
	childHeadersEndOffsetX := nonLeafHeaderSize + numberOfChildrenX*nonLeafChildHeaderSize
	keysOffsetX := int(nonLeafChildHeader(nlc[nonLeafHeaderSize:]).KeyOffset())
	keysOffset := int(nonLeafChildHeader(nlc[childHeadersOffset:]).KeyOffset())
	var keysEndOffset int

	if childHeadersEndOffset == childHeadersEndOffsetX {
		keysEndOffset = nonLeafSize
	} else {
		keysEndOffset = int(nonLeafChildHeader(nlc[childHeadersEndOffset:]).KeyOffset())
	}

	keysSize := keysEndOffset - keysOffset

	children := nlc.doRemoveChildren(
		numberOfChildren,
		childHeadersEndOffset,
		childHeadersEndOffsetX,
		childHeadersSize,
		keysOffsetX,
		keysOffset,
		keysSize,
	)

	nonLeafHeader(nlc).SetChildCount(int32(numberOfChildrenX - numberOfChildren))
	return children
}

func (nlc nonLeafController) CountChildrenForSpliting() int {
	n := nlc.NumberOfChildren()
	lastChildSize := nonLeafChildHeaderSize + len(nlc.GetKey(n-1))
	loadSize1 := nlc.GetLoadSize() - lastChildSize
	loadSize2 := nonLeafChildHeaderSize
	childCount := 0

	for i := n - 2; ; i-- {
		if loadSize1 < nonLeafUnderloadThreshold {
			break
		}

		childCount++

		if loadSize1 <= loadSize2 {
			break
		}

		childSize := nonLeafChildHeaderSize + len(nlc.GetKey(i))
		loadSize1 -= childSize
		loadSize2 += lastChildSize
		lastChildSize = childSize

	}

	return childCount
}

func (nlc nonLeafController) Split(numberOfChildren int, parent nonLeafController, index int, newSibling nonLeafController, newSiblingAddr int64) {
	children := nlc.RemoveChildren(nlc.NumberOfChildren()-numberOfChildren, numberOfChildren)
	key := children[0].Key
	children[0].Key = nil
	newSibling.InsertChildren(0, children)
	parent.InsertChildren(index+1, []nonLeafChild{{key, newSiblingAddr}})
}

func (nlc nonLeafController) MergeToLeft(parent nonLeafController, index int, leftSibling nonLeafController) {
	leftSibling.MergeFromRight(parent, index-1, nlc)
}

func (nlc nonLeafController) MergeFromRight(parent nonLeafController, index int, rightSibling nonLeafController) {
	key := parent.RemoveChildren(index+1, 1)[0].Key
	children := rightSibling.RemoveChildren(0, rightSibling.NumberOfChildren())
	children[0].Key = key
	nlc.InsertChildren(nlc.NumberOfChildren(), children)
}

func (nlc nonLeafController) CountChildrenForShiftingToLeft(leftSibling nonLeafController) int {
	loadSize1 := nlc.GetLoadSize() - (nonLeafChildHeaderSize + len(nlc.GetKey(0)))
	loadSize2 := leftSibling.GetLoadSize() + nonLeafChildHeaderSize
	childCount := 0

	for i := 1; ; i++ {
		if loadSize1 < nonLeafUnderloadThreshold || loadSize2 > nonLeafOverloadThreshold {
			break
		}

		childCount++

		if loadSize1 <= loadSize2 {
			break
		}

		childSize := nonLeafChildHeaderSize + len(nlc.GetKey(i))
		loadSize1 -= childSize
		loadSize2 += childSize
	}

	if loadSize1 > nonLeafOverloadThreshold || loadSize2 < nonLeafUnderloadThreshold {
		return 0
	}

	return childCount
}

func (nlc nonLeafController) ShiftToLeft(numberOfChildren int, parent nonLeafController, index int, leftSibling nonLeafController) {
	children := nlc.RemoveChildren(0, numberOfChildren)
	children[0].Key = copyBytes(parent.GetKey(index))
	parent.SetKey(index, nlc.GetKey(0))
	nlc.SetKey(0, nil)
	leftSibling.InsertChildren(leftSibling.NumberOfChildren(), children)
}

func (nlc nonLeafController) CountChildrenForShiftingToRight(rightSibling nonLeafController) int {
	n := nlc.NumberOfChildren()
	lastChildSize := nonLeafChildHeaderSize + len(nlc.GetKey(n-1))
	loadSize1 := nlc.GetLoadSize() - lastChildSize
	loadSize2 := rightSibling.GetLoadSize() + nonLeafChildHeaderSize
	childCount := 0

	for i := n - 2; ; i-- {
		if loadSize1 < nonLeafUnderloadThreshold || loadSize2 > nonLeafOverloadThreshold {
			break
		}

		childCount++

		if loadSize1 <= loadSize2 {
			break
		}

		childSize := nonLeafChildHeaderSize + len(nlc.GetKey(i))
		loadSize1 -= childSize
		loadSize2 += lastChildSize
		lastChildSize = childSize
	}

	if loadSize1 > nonLeafOverloadThreshold || loadSize2 < nonLeafUnderloadThreshold {
		return 0
	}

	return childCount
}

func (nlc nonLeafController) ShiftToRight(numberOfChildren int, parent nonLeafController, index int, rightSibling nonLeafController) {
	children := nlc.RemoveChildren(nlc.NumberOfChildren()-numberOfChildren, numberOfChildren)
	rightSibling.SetKey(0, parent.GetKey(index+1))
	parent.SetKey(index+1, children[0].Key)
	children[0].Key = nil
	rightSibling.InsertChildren(0, children)
}

func (nlc nonLeafController) CountChildrenForUnshiftingFromLeft(leftSibling nonLeafController) int {
	return leftSibling.CountChildrenForShiftingToRight(nlc)
}

func (nlc nonLeafController) UnshiftFromLeft(numberOfChildren int, parent nonLeafController, index int, leftSibling nonLeafController) {
	leftSibling.ShiftToRight(numberOfChildren, parent, index-1, nlc)
}

func (nlc nonLeafController) CountChildrenForUnshiftingFromRight(rightSibling nonLeafController) int {
	return rightSibling.CountChildrenForShiftingToLeft(nlc)
}

func (nlc nonLeafController) UnshiftFromRight(numberOfChildren int, parent nonLeafController, index int, rightSibling nonLeafController) {
	rightSibling.ShiftToLeft(numberOfChildren, parent, index+1, nlc)
}

func (nlc nonLeafController) GetLoadSize() int {
	numberOfChildrenX := nlc.NumberOfChildren()
	var keysOffsetX int

	if numberOfChildrenX == 0 {
		keysOffsetX = nonLeafSize
	} else {
		keysOffsetX = int(nonLeafChildHeader(nlc[nonLeafHeaderSize:]).KeyOffset())
	}

	childHeadersEndOffsetX := nonLeafHeaderSize + numberOfChildrenX*nonLeafChildHeaderSize
	freeSpaceSize := keysOffsetX - childHeadersEndOffsetX
	return maxNonLeafFreeSpaceSize - freeSpaceSize
}

func (nlc nonLeafController) SetKey(childIndex int, key key) {
	numberOfChildren := nlc.checkChildIndex(childIndex)
	keysOffsetX := int(nonLeafChildHeader(nlc[nonLeafHeaderSize:]).KeyOffset())
	childHeaderOffset := nonLeafHeaderSize + childIndex*nonLeafChildHeaderSize
	childHeader := nonLeafChildHeader(nlc[childHeaderOffset:])
	keyOffset := int(childHeader.KeyOffset())
	var keyEndOffset int

	if childIndex+1 == numberOfChildren {
		keyEndOffset = nonLeafSize
	} else {
		keyEndOffset = int(nonLeafChildHeader(nlc[childHeaderOffset+nonLeafChildHeaderSize:]).KeyOffset())
	}

	keySizeDelta := len(key) - (keyEndOffset - keyOffset)

	nlc.doSetKey(
		keysOffsetX,
		keyOffset,
		keySizeDelta,
		key,
		childHeaderOffset,
	)
}

func (nlc nonLeafController) GetKey(childIndex int) key {
	numberOfChildren := nlc.checkChildIndex(childIndex)
	childHeaderOffset := nonLeafHeaderSize + childIndex*nonLeafChildHeaderSize
	childHeader := nonLeafChildHeader(nlc[childHeaderOffset:])
	keyOffset := int(childHeader.KeyOffset())
	var keyEndOffset int

	if childIndex+1 == numberOfChildren {
		keyEndOffset = nonLeafSize
	} else {
		keyEndOffset = int(nonLeafChildHeader(nlc[childHeaderOffset+nonLeafChildHeaderSize:]).KeyOffset())
	}

	return key(nlc[keyOffset:keyEndOffset])
}

func (nlc nonLeafController) GetChildAddr(childIndex int) int64 {
	nlc.checkChildIndex(childIndex)
	childHeader := nonLeafChildHeader(nlc[nonLeafHeaderSize+childIndex*nonLeafChildHeaderSize:])
	return childHeader.Addr()
}

func (nlc nonLeafController) NumberOfChildren() int {
	return int(nonLeafHeader(nlc).ChildCount())
}

func (nlc nonLeafController) checkChildIndex(childIndex int) int {
	numberOfChildren := nlc.NumberOfChildren()

	if childIndex < 0 || childIndex > numberOfChildren {
		panic(errOutOfRange)
	}

	return numberOfChildren
}

func (nlc nonLeafController) doInsertChildren(
	childHeadersOffset int,
	childHeadersEndOffsetX int,
	childHeadersSize int,
	keysOffsetX int,
	keysEndOffset int,
	keysSize int,
	children []nonLeafChild,
) {
	copy(nlc[childHeadersOffset+childHeadersSize:], nlc[childHeadersOffset:childHeadersEndOffsetX])
	copy(nlc[keysOffsetX-keysSize:], nlc[keysOffsetX:keysEndOffset])

	for i := nonLeafHeaderSize; i < childHeadersOffset; i += nonLeafChildHeaderSize {
		childHeader := nonLeafChildHeader(nlc[i:])
		keyOffset := int(childHeader.KeyOffset())
		childHeader.SetKeyOffset(int32(keyOffset - keysSize))
	}

	keysOffset := keysEndOffset - keysSize

	for _, child := range children {
		keyOffset := keysOffset
		keysOffset += copy(nlc[keysOffset:], child.Key)
		childHeaderOffset := childHeadersOffset
		childHeadersOffset += nonLeafChildHeaderSize
		childHeader := nonLeafChildHeader(nlc[childHeaderOffset:])
		childHeader.SetKeyOffset(int32(keyOffset))
		childHeader.SetAddr(child.Addr)
	}
}

func (nlc nonLeafController) doRemoveChildren(
	numberOfChildren int,
	childHeadersEndOffset int,
	childHeadersEndOffsetX int,
	childHeadersSize int,
	keysOffsetX int,
	keysOffset int,
	keysSize int,
) []nonLeafChild {
	childHeadersOffset := childHeadersEndOffset - childHeadersSize
	keys := make([]byte, keysSize)
	children := make([]nonLeafChild, 0, numberOfChildren)

	for i := childHeadersOffset; i < childHeadersEndOffset; i += nonLeafChildHeaderSize {
		childHeader := nonLeafChildHeader(nlc[i:])
		keyOffset := int(childHeader.KeyOffset())
		var keyEndOffset int

		if j := i + nonLeafChildHeaderSize; j == childHeadersEndOffsetX {
			keyEndOffset = nonLeafSize
		} else {
			keyEndOffset = int(nonLeafChildHeader(nlc[j:]).KeyOffset())
		}

		key := keys[:keyEndOffset-keyOffset]
		keys = keys[len(key):]
		copy(key, nlc[keyOffset:])
		children = append(children, nonLeafChild{key, childHeader.Addr()})
	}

	copy(nlc[childHeadersEndOffset-childHeadersSize:], nlc[childHeadersEndOffset:childHeadersEndOffsetX])
	copy(nlc[keysOffsetX+keysSize:], nlc[keysOffsetX:keysOffset])

	for i := nonLeafHeaderSize; i < childHeadersOffset; i += nonLeafChildHeaderSize {
		childHeader := nonLeafChildHeader(nlc[i:])
		keyOffset := int(childHeader.KeyOffset())
		childHeader.SetKeyOffset(int32(keyOffset + keysSize))
	}

	return children
}

func (nlc nonLeafController) doSetKey(
	keysOffsetX int,
	keyOffset int,
	keySizeDelta int,
	key key,
	childHeaderOffset int,
) {
	copy(nlc[keysOffsetX-keySizeDelta:], nlc[keysOffsetX:keyOffset])
	copy(nlc[keyOffset-keySizeDelta:], key)

	for i := nonLeafHeaderSize; i <= childHeaderOffset; i += nonLeafChildHeaderSize {
		childHeader := nonLeafChildHeader(nlc[i:])
		keyOffset := int(childHeader.KeyOffset())
		childHeader.SetKeyOffset(int32(keyOffset - keySizeDelta))
	}
}

type nonLeafChild struct {
	Key  key
	Addr int64
}

type nonLeafHeader []byte

func (nlh nonLeafHeader) SetChildCount(value int32) {
	binary.BigEndian.PutUint32(nlh[0:], uint32(value))
}

func (nlh nonLeafHeader) ChildCount() int32 {
	return int32(binary.BigEndian.Uint32(nlh[0:]))
}

const nonLeafHeaderSize = 4

type nonLeafChildHeader []byte

func (nlch nonLeafChildHeader) SetKeyOffset(value int32) {
	binary.BigEndian.PutUint32(nlch[0:], uint32(value))
}

func (nlch nonLeafChildHeader) KeyOffset() int32 {
	return int32(binary.BigEndian.Uint32(nlch[0:]))
}

func (nlch nonLeafChildHeader) SetAddr(value int64) {
	binary.BigEndian.PutUint64(nlch[4:], uint64(value))
}

func (nlch nonLeafChildHeader) Addr() int64 {
	return int64(binary.BigEndian.Uint64(nlch[4:]))
}

const nonLeafChildHeaderSize = 12
