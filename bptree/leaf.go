package bptree

import (
	"encoding/binary"

	"github.com/roy2220/fsm"
)

const (
	leafSize               = 1 << 13
	maxLeafFreeSpaceSize   = leafSize - leafHeaderSize
	maxRecordSize          = recordHeaderSize + maxKeySize + maxValueSize
	leafOverloadThreshold  = maxLeafFreeSpaceSize - maxRecordSize
	leafUnderloadThreshold = (leafOverloadThreshold-maxRecordSize)/2 + 1
)

type leafFactory struct {
	FileStorage *fsm.FileStorage
}

func (lf leafFactory) CreateLeaf() (int64, leafController) {
	leafAddr, leafAccessor := lf.FileStorage.AllocateAlignedSpace(leafSize)

	for i := 0; i < leafHeaderSize; i++ {
		leafAccessor[i] = 0
	}

	return leafAddr, leafController(leafAccessor)
}

func (lf leafFactory) DestroyLeaf(leafAddr int64) {
	lf.FileStorage.FreeAlignedSpace(leafAddr)
}

func (lf leafFactory) GetLeafController(leafAddr int64) leafController {
	return leafController(lf.FileStorage.AccessAlignedSpace(leafAddr))
}

type leafController []byte

func (lc leafController) LocateRecord(key []byte, keyComparer keyComparer) (int, bool) {
	if isMinKey(key) {
		return 0, true
	}

	n := lc.NumberOfRecords()

	if isMaxKey(key) {
		return n - 1, true
	}

	i, j := 0, n-1

	for i < j {
		k := (i + j) / 2
		// i <= k < j

		if keyComparer.CompareKey(lc.GetKey(k), key) < 0 {
			i = k + 1
			// i <= j
		} else {
			j = k
			// j >= i
		}
	}
	// i == j

	d := keyComparer.CompareKey(lc.GetKey(i), key)

	if d == 0 {
		return i, true
	}

	if d < 0 && i == n-1 {
		i = n
	}

	return i, false
}

func (lc leafController) InsertRecords(firstRecordIndex int, records []record) {
	numberOfRecordsX := lc.checkRecordIndex(firstRecordIndex)
	recordHeadersOffset := leafHeaderSize + firstRecordIndex*recordHeaderSize
	recordHeadersEndOffsetX := leafHeaderSize + numberOfRecordsX*recordHeaderSize
	recordHeadersSize := len(records) * recordHeaderSize
	var kvsOffsetX int

	if numberOfRecordsX == 0 {
		kvsOffsetX = leafSize
	} else {
		kvsOffsetX = int(recordHeader(lc[leafHeaderSize:]).KeyOffset())
	}

	var kvsEndOffset int

	if recordHeadersOffset == recordHeadersEndOffsetX {
		kvsEndOffset = leafSize
	} else {
		kvsEndOffset = int(recordHeader(lc[recordHeadersOffset:]).KeyOffset())
	}

	kvsSize := 0

	for _, record := range records {
		kvsSize += len(record.Key) + len(record.Value)
	}

	lc.doInsertRecords(
		recordHeadersOffset,
		recordHeadersEndOffsetX,
		recordHeadersSize,
		kvsOffsetX,
		kvsEndOffset,
		kvsSize,
		records,
	)

	leafHeader(lc).SetRecordCount(int32(numberOfRecordsX + len(records)))
}

func (lc leafController) RemoveRecords(firstRecordIndex int, numberOfRecords int) []record {
	numberOfRecordsX := lc.checkRecordIndex(firstRecordIndex)

	if firstRecordIndex+numberOfRecords > numberOfRecordsX {
		panic(errOutOfRange)
	}

	recordHeadersOffset := leafHeaderSize + firstRecordIndex*recordHeaderSize
	recordHeadersSize := numberOfRecords * recordHeaderSize
	recordHeadersEndOffset := recordHeadersOffset + recordHeadersSize
	recordHeadersEndOffsetX := leafHeaderSize + numberOfRecordsX*recordHeaderSize
	kvsOffsetX := int(recordHeader(lc[leafHeaderSize:]).KeyOffset())
	kvsOffset := int(recordHeader(lc[recordHeadersOffset:]).KeyOffset())
	var kvsEndOffset int

	if recordHeadersEndOffset == recordHeadersEndOffsetX {
		kvsEndOffset = leafSize
	} else {
		kvsEndOffset = int(recordHeader(lc[recordHeadersEndOffset:]).KeyOffset())
	}

	kvsSize := kvsEndOffset - kvsOffset

	records := lc.doRemoveRecords(
		numberOfRecords,
		recordHeadersEndOffset,
		recordHeadersEndOffsetX,
		recordHeadersSize,
		kvsOffsetX,
		kvsOffset,
		kvsSize,
	)

	leafHeader(lc).SetRecordCount(int32(numberOfRecordsX - numberOfRecords))
	return records
}

func (lc leafController) CountRecordsForSpliting() int {
	loadSize1 := lc.GetLoadSize()
	loadSize2 := 0
	recordCount := 0

	for i := lc.NumberOfRecords() - 1; ; i-- {
		recordSize := recordHeaderSize + len(lc.GetKey(i)) + len(lc.GetValue(i))
		loadSize1 -= recordSize
		loadSize2 += recordSize

		if loadSize1 < leafUnderloadThreshold {
			break
		}

		recordCount++

		if loadSize1 <= loadSize2 {
			break
		}
	}

	return recordCount
}

func (lc leafController) Split(numberOfRecords int, parent nonLeafController, index int, newSibling leafController, newSiblingAddr int64) {
	records := lc.RemoveRecords(lc.NumberOfRecords()-numberOfRecords, numberOfRecords)
	newSibling.InsertRecords(0, records)
	parent.InsertChildren(index+1, []nonLeafChild{{records[0].Key, newSiblingAddr}})
}

func (lc leafController) MergeToLeft(parent nonLeafController, index int, leftSibling leafController) {
	leftSibling.MergeFromRight(parent, index-1, lc)
}

func (lc leafController) MergeFromRight(parent nonLeafController, index int, rightSibling leafController) {
	parent.RemoveChildren(index+1, 1)
	records := rightSibling.RemoveRecords(0, rightSibling.NumberOfRecords())
	lc.InsertRecords(lc.NumberOfRecords(), records)
}

func (lc leafController) CountRecordsForShiftingToLeft(leftSibling leafController) int {
	loadSize1 := lc.GetLoadSize()
	loadSize2 := leftSibling.GetLoadSize()
	recordCount := 0

	for i := 0; ; i++ {
		recordSize := recordHeaderSize + len(lc.GetKey(i)) + len(lc.GetValue(i))
		loadSize1 -= recordSize
		loadSize2 += recordSize

		if loadSize1 < leafUnderloadThreshold || loadSize2 > leafOverloadThreshold {
			break
		}

		recordCount++

		if loadSize1 <= loadSize2 {
			break
		}
	}

	if loadSize1 > leafOverloadThreshold || loadSize2 < leafUnderloadThreshold {
		return 0
	}

	return recordCount
}

func (lc leafController) ShiftToLeft(numberOfRecords int, parent nonLeafController, index int, leftSibling leafController) {
	records := lc.RemoveRecords(0, numberOfRecords)
	parent.SetKey(index, lc.GetKey(0))
	leftSibling.InsertRecords(leftSibling.NumberOfRecords(), records)
}

func (lc leafController) CountRecordsForShiftingToRight(rightSibling leafController) int {
	loadSize1 := lc.GetLoadSize()
	loadSize2 := rightSibling.GetLoadSize()
	recordCount := 0

	for i := lc.NumberOfRecords() - 1; ; i-- {
		recordSize := recordHeaderSize + len(lc.GetKey(i)) + len(lc.GetValue(i))
		loadSize1 -= recordSize
		loadSize2 += recordSize

		if loadSize1 < leafUnderloadThreshold || loadSize2 > leafOverloadThreshold {
			break
		}

		recordCount++

		if loadSize1 <= loadSize2 {
			break
		}
	}

	if loadSize1 > leafOverloadThreshold || loadSize2 < leafUnderloadThreshold {
		return 0
	}

	return recordCount
}

func (lc leafController) ShiftToRight(numberOfRecords int, parent nonLeafController, index int, rightSibling leafController) {
	records := lc.RemoveRecords(lc.NumberOfRecords()-numberOfRecords, numberOfRecords)
	parent.SetKey(index+1, records[0].Key)
	rightSibling.InsertRecords(0, records)
}

func (lc leafController) CountRecordsForUnshiftingFromLeft(leftSibling leafController) int {
	return leftSibling.CountRecordsForShiftingToRight(lc)
}

func (lc leafController) UnshiftFromLeft(numberOfRecords int, parent nonLeafController, index int, leftSibling leafController) {
	leftSibling.ShiftToRight(numberOfRecords, parent, index-1, lc)
}

func (lc leafController) CountRecordsForUnshiftingFromRight(rightSibling leafController) int {
	return rightSibling.CountRecordsForShiftingToLeft(lc)
}

func (lc leafController) UnshiftFromRight(numberOfRecords int, parent nonLeafController, index int, rightSibling leafController) {
	rightSibling.ShiftToLeft(numberOfRecords, parent, index+1, lc)
}

func (lc leafController) GetLoadSize() int {
	numberOfRecordsX := lc.NumberOfRecords()
	var kvsOffsetX int

	if numberOfRecordsX == 0 {
		kvsOffsetX = leafSize
	} else {
		kvsOffsetX = int(recordHeader(lc[leafHeaderSize:]).KeyOffset())
	}

	recordHeadersEndOffsetX := leafHeaderSize + numberOfRecordsX*recordHeaderSize
	freeSpaceSize := kvsOffsetX - recordHeadersEndOffsetX
	return maxLeafFreeSpaceSize - freeSpaceSize
}

func (lc leafController) SetValue(recordIndex int, value value) {
	numberOfRecords := lc.checkRecordIndex(recordIndex)
	kvsOffsetX := int(recordHeader(lc[leafHeaderSize:]).KeyOffset())
	recordHeaderOffset := leafHeaderSize + recordIndex*recordHeaderSize
	recordHeader1 := recordHeader(lc[recordHeaderOffset:])
	valueOffset := int(recordHeader1.ValueOffset())
	var valueEndOffset int

	if recordIndex+1 == numberOfRecords {
		valueEndOffset = leafSize
	} else {
		valueEndOffset = int(recordHeader(lc[recordHeaderOffset+recordHeaderSize:]).KeyOffset())
	}

	valueSizeDelta := len(value) - (valueEndOffset - valueOffset)

	lc.doSetValue(
		kvsOffsetX,
		valueOffset,
		valueSizeDelta,
		value,
		recordHeaderOffset,
	)
}

func (lc leafController) GetKey(recordIndex int) key {
	lc.checkRecordIndex(recordIndex)
	recordHeader1 := recordHeader(lc[leafHeaderSize+recordIndex*recordHeaderSize:])
	keyOffset := int(recordHeader1.KeyOffset())
	keyEndOffset := int(recordHeader1.ValueOffset())
	return key(lc[keyOffset:keyEndOffset])
}

func (lc leafController) GetValue(recordIndex int) value {
	numberOfRecords := lc.checkRecordIndex(recordIndex)
	recordHeaderOffset := leafHeaderSize + recordIndex*recordHeaderSize
	recordHeader1 := recordHeader(lc[recordHeaderOffset:])
	valueOffset := int(recordHeader1.ValueOffset())
	var valueEndOffset int

	if recordIndex+1 == numberOfRecords {
		valueEndOffset = leafSize
	} else {
		valueEndOffset = int(recordHeader(lc[recordHeaderOffset+recordHeaderSize:]).KeyOffset())
	}

	return value(lc[valueOffset:valueEndOffset])
}

func (lc leafController) NumberOfRecords() int {
	return int(leafHeader(lc).RecordCount())
}

func (lc leafController) checkRecordIndex(recordIndex int) int {
	numberOfRecords := lc.NumberOfRecords()

	if recordIndex < 0 || recordIndex > numberOfRecords {
		panic(errOutOfRange)
	}

	return numberOfRecords
}

func (lc leafController) doInsertRecords(
	recordHeadersOffset int,
	recordHeadersEndOffsetX int,
	recordHeadersSize int,
	kvsOffsetX int,
	kvsEndOffset int,
	kvsSize int,
	records []record,
) {
	copy(lc[recordHeadersOffset+recordHeadersSize:], lc[recordHeadersOffset:recordHeadersEndOffsetX])
	copy(lc[kvsOffsetX-kvsSize:], lc[kvsOffsetX:kvsEndOffset])

	for i := leafHeaderSize; i < recordHeadersOffset; i += recordHeaderSize {
		recordHeader := recordHeader(lc[i:])
		keyOffset := int(recordHeader.KeyOffset())
		recordHeader.SetKeyOffset(int32(keyOffset - kvsSize))
		valueOffset := int(recordHeader.ValueOffset())
		recordHeader.SetValueOffset(int32(valueOffset - kvsSize))
	}

	kvsOffset := kvsEndOffset - kvsSize

	for _, record := range records {
		keyOffset := kvsOffset
		kvsOffset += copy(lc[kvsOffset:], record.Key)
		valueOffset := kvsOffset
		kvsOffset += copy(lc[kvsOffset:], record.Value)
		recordHeaderOffset := recordHeadersOffset
		recordHeadersOffset += recordHeaderSize
		recordHeader := recordHeader(lc[recordHeaderOffset:])
		recordHeader.SetKeyOffset(int32(keyOffset))
		recordHeader.SetValueOffset(int32(valueOffset))
	}
}

func (lc leafController) doRemoveRecords(
	numberOfRecords int,
	recordHeadersEndOffset int,
	recordHeadersEndOffsetX int,
	recordHeadersSize int,
	kvsOffsetX int,
	kvsOffset int,
	kvsSize int,
) []record {
	recordHeadersOffset := recordHeadersEndOffset - recordHeadersSize
	kvs := make([]byte, kvsSize)
	records := make([]record, 0, numberOfRecords)

	for i := recordHeadersOffset; i < recordHeadersEndOffset; i += recordHeaderSize {
		recordHeader1 := recordHeader(lc[i:])
		keyOffset := int(recordHeader1.KeyOffset())
		keyEndOffset := int(recordHeader1.ValueOffset())
		key := kvs[:keyEndOffset-keyOffset]
		kvs = kvs[len(key):]
		copy(key, lc[keyOffset:])
		valueOffset := keyEndOffset
		var valueEndOffset int

		if j := i + recordHeaderSize; j == recordHeadersEndOffsetX {
			valueEndOffset = leafSize
		} else {
			valueEndOffset = int(recordHeader(lc[j:]).KeyOffset())
		}

		value := kvs[:valueEndOffset-valueOffset]
		kvs = kvs[len(value):]
		copy(value, lc[valueOffset:])
		records = append(records, record{key, value})
	}

	copy(lc[recordHeadersEndOffset-recordHeadersSize:], lc[recordHeadersEndOffset:recordHeadersEndOffsetX])
	copy(lc[kvsOffsetX+kvsSize:], lc[kvsOffsetX:kvsOffset])

	for i := leafHeaderSize; i < recordHeadersOffset; i += recordHeaderSize {
		recordHeader := recordHeader(lc[i:])
		keyOffset := int(recordHeader.KeyOffset())
		recordHeader.SetKeyOffset(int32(keyOffset + kvsSize))
		valueOffset := int(recordHeader.ValueOffset())
		recordHeader.SetValueOffset(int32(valueOffset + kvsSize))
	}

	return records
}

func (lc leafController) doSetValue(
	kvsOffsetX int,
	valueOffset int,
	valueSizeDelta int,
	value value,
	recordHeaderOffset int,
) {
	copy(lc[kvsOffsetX-valueSizeDelta:], lc[kvsOffsetX:valueOffset])
	copy(lc[valueOffset-valueSizeDelta:], value)

	for i := leafHeaderSize; i <= recordHeaderOffset; i += recordHeaderSize {
		recordHeader := recordHeader(lc[i:])
		keyOffset := int(recordHeader.KeyOffset())
		recordHeader.SetKeyOffset(int32(keyOffset - valueSizeDelta))
		valueOffset := int(recordHeader.ValueOffset())
		recordHeader.SetValueOffset(int32(valueOffset - valueSizeDelta))
	}
}

type record struct {
	Key   key
	Value value
}

type leafHeader []byte

func (lh leafHeader) SetPrevAddr(value int64) {
	binary.BigEndian.PutUint64(lh[0:], uint64(value))
}

func (lh leafHeader) PrevAddr() int64 {
	return int64(binary.BigEndian.Uint64(lh[0:]))
}

func (lh leafHeader) SetNextAddr(value int64) {
	binary.BigEndian.PutUint64(lh[8:], uint64(value))
}

func (lh leafHeader) NextAddr() int64 {
	return int64(binary.BigEndian.Uint64(lh[8:]))
}

func (lh leafHeader) SetRecordCount(value int32) {
	binary.BigEndian.PutUint32(lh[16:], uint32(value))
}

func (lh leafHeader) RecordCount() int32 {
	return int32(binary.BigEndian.Uint32(lh[16:]))
}

const leafHeaderSize = 20

type recordHeader []byte

func (rh recordHeader) SetKeyOffset(value int32) {
	binary.BigEndian.PutUint32(rh[0:], uint32(value))
}

func (rh recordHeader) KeyOffset() int32 {
	return int32(binary.BigEndian.Uint32(rh[0:]))
}

func (rh recordHeader) SetValueOffset(value int32) {
	binary.BigEndian.PutUint32(rh[4:], uint32(value))
}

func (rh recordHeader) ValueOffset() int32 {
	return int32(binary.BigEndian.Uint32(rh[4:]))
}

const recordHeaderSize = 8
