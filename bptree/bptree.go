// Package bptree implements an on-disk B+ tree.
package bptree

import (
	"bytes"

	"github.com/roy2220/fsm"
)

// BPTree represents a B+ tree on disk.
type BPTree struct {
	fileStorage  *fsm.FileStorage
	rootAddr     int64
	height       int
	leafList     leafList
	leafCount    int
	nonLeafCount int
	recordCount  int
	payloadSize  int
}

// Init initializes the B+ tree with the given file storage and returns it.
func (bpt *BPTree) Init(fileStorage *fsm.FileStorage) *BPTree {
	bpt.fileStorage = fileStorage
	bpt.rootAddr = -1
	return bpt
}

// Create creates the B+ tree on the file storage.
func (bpt *BPTree) Create() {
	var rootController leafController
	bpt.rootAddr, rootController = bpt.createLeaf()
	bpt.height = 1
	bpt.leafList.Init(rootController, bpt.rootAddr)
}

// Destroy destroys the B+ tree on the file storage.
func (bpt *BPTree) Destroy() {
	bpt.destroyLeaf(bpt.rootAddr)
	*bpt = *new(BPTree).Init(bpt.fileStorage)
}

// AddRecord adds the given record to the B+ tree.
// If no record with an identical key exists in the B+ tree,
// it adds the record and then returns true, otherwise it
// returns false and the present value (optional) of the record.
func (bpt *BPTree) AddRecord(key, value []byte, returnPresentValue bool) ([]byte, bool) {
	recordPath, ok := bpt.findRecord(key)

	if ok {
		return bpt.getValue(recordPath, returnPresentValue), false
	}

	bpt.insertRecord(recordPath, bpt.createRecord(key, value))
	return nil, true
}

// UpdateRecord replaces the value of a record with the given
// key in the B+ tree to the given value.
// If a record with an identical key exists in the B+ tree,
// it updates the record and then returns true and the replaced
// value (optional) of the record, otherwise it returns flase.
func (bpt *BPTree) UpdateRecord(key, value []byte, returnReplacedValue bool) ([]byte, bool) {
	recordPath, ok := bpt.findRecord(key)

	if !ok {
		return nil, false
	}

	return bpt.replaceValue(recordPath, value, returnReplacedValue), true
}

// AddOrUpdateRecord adds the given record to the B+ tree or
// replaces the value of a record with the given key to the
// given value.
// If no record with an identical key exists in the B+ tree,
// it adds the record and then returns true, otherwise if
// updates the record and then returns false and the replaced
// value (optional) of the record.
func (bpt *BPTree) AddOrUpdateRecord(key, value []byte, returnReplacedValue bool) ([]byte, bool) {
	recordPath, ok := bpt.findRecord(key)

	if ok {
		return bpt.replaceValue(recordPath, value, returnReplacedValue), false
	}

	bpt.insertRecord(recordPath, bpt.createRecord(key, value))
	return nil, true
}

// DeleteRecord deletes a record with the given key in the
// B+ tree.
// If a record with an identical key exists in the B+ tree,
// it deletes the record then then returns true and the
// removed value (optional) of the record, otherwise it
// returns flase.
func (bpt *BPTree) DeleteRecord(key []byte, returnRemovedValue bool) ([]byte, bool) {
	recordPath, ok := bpt.findRecord(key)

	if !ok {
		return nil, false
	}

	return bpt.destroyRecord(bpt.removeRecord(recordPath), returnRemovedValue), true
}

// HasRecord checks whether a record with the given key
// in the B+ tree.
// If a record with an identical key exists in the B+ tree,
// it returns true and the present value (optional) of the
// record, otherwise it returns flase.
func (bpt *BPTree) HasRecord(key []byte, returnPresentValue bool) ([]byte, bool) {
	recordPath, ok := bpt.findRecord(key)

	if !ok {
		return nil, false
	}

	return bpt.getValue(recordPath, returnPresentValue), true
}

// SearchForward searchs the the B+ tree for records with
// keys in the given interval [maxKey, minKey].
// It returns an iterator to iterate over the records found
// in ascending order.
func (bpt *BPTree) SearchForward(minKey []byte, maxKey []byte) Iterator {
	minLeafAddr, minRecordIndex, maxLeafAddr, maxRecordIndex, ok := bpt.search(minKey, maxKey)

	return new(forwardIterator).Init(
		bpt.fileStorage,
		minLeafAddr,
		minRecordIndex,
		maxLeafAddr,
		maxRecordIndex,
		!ok,
	)
}

// SearchBackward searchs the the B+ tree for records with
// keys in the given interval [maxKey, minKey].
// It returns an iterator to iterate over the records found
// in descending order.
func (bpt *BPTree) SearchBackward(minKey []byte, maxKey []byte) Iterator {
	minLeafAddr, minRecordIndex, maxLeafAddr, maxRecordIndex, ok := bpt.search(minKey, maxKey)

	return new(backwardIterator).Init(
		bpt.fileStorage,
		maxLeafAddr,
		maxRecordIndex,
		minLeafAddr,
		minRecordIndex,
		!ok,
	)
}

// Height returns the height of the B+ tree.
func (bpt *BPTree) Height() int {
	return bpt.height
}

// NumberOfLeafs returns the number of leafs in the B+ tree.
func (bpt *BPTree) NumberOfLeafs() int {
	return bpt.leafCount
}

// NumberOfNonLeafs returns the number of non-leafs in the B+ tree.
func (bpt *BPTree) NumberOfNonLeafs() int {
	return bpt.nonLeafCount
}

// NumberOfRecords returns the number of records in the B+ tree.
func (bpt *BPTree) NumberOfRecords() int {
	return bpt.recordCount
}

// PayloadSize returns the payload size of the B+ tree.
func (bpt *BPTree) PayloadSize() int {
	return bpt.payloadSize
}

func (bpt *BPTree) insertRecord(recordPath recordPath, record1 record) {
	_, leafController, recordIndex := bpt.locateRecord(recordPath)
	leafController.InsertRecords(recordIndex, []record{record1})
	bpt.syncKey(&recordPath)
	bpt.ensureNotOverloadLeaf(&recordPath)
	bpt.recordCount++
}

func (bpt *BPTree) removeRecord(recordPath recordPath) record {
	_, leafController, recordIndex := bpt.locateRecord(recordPath)
	record := leafController.RemoveRecords(recordIndex, 1)[0]
	bpt.syncKey(&recordPath)
	bpt.ensureNotUnderloadLeaf(&recordPath)
	bpt.recordCount--
	return record
}

func (bpt *BPTree) getValue(recordPath recordPath, do bool) []byte {
	if !do {
		return nil
	}

	_, leafController, recordIndex := bpt.locateRecord(recordPath)
	value := leafController.GetValue(recordIndex)
	return valueFactory{bpt.fileStorage}.ReadValue(value)
}

func (bpt *BPTree) createRecord(key, value []byte) record {
	record := record{
		Key:   keyFactory{bpt.fileStorage}.CreateKey(key),
		Value: valueFactory{bpt.fileStorage}.CreateValue(value),
	}

	bpt.payloadSize += len(key) + len(value)
	return record
}

func (bpt *BPTree) destroyRecord(record record, returnValue bool) []byte {
	keySize := keyFactory{bpt.fileStorage}.DestroyKey(record.Key)
	var value []byte

	if returnValue {
		value = valueFactory{bpt.fileStorage}.ReadValue(record.Value)
	} else {
		value = nil
	}

	valueSize := valueFactory{bpt.fileStorage}.DestroyValue(record.Value)
	bpt.payloadSize -= keySize + valueSize
	return value
}

func (bpt *BPTree) replaceValue(recordPath recordPath, newValue []byte, returnOldValue bool) []byte {
	_, leafController, recordIndex := bpt.locateRecord(recordPath)
	value := leafController.GetValue(recordIndex)
	var oldValue []byte
	var oldValueSize int

	if returnOldValue {
		oldValue = valueFactory{bpt.fileStorage}.ReadValue(value)
		oldValueSize = len(oldValue)
	} else {
		oldValue = nil
		oldValueSize = valueFactory{bpt.fileStorage}.GetValueSize(value)
	}

	valueFactory{bpt.fileStorage}.DestroyValue(value)
	value = valueFactory{bpt.fileStorage}.CreateValue(newValue)
	leafController.SetValue(recordIndex, value)
	bpt.ensureNotUnderloadLeaf(&recordPath)
	bpt.ensureNotOverloadLeaf(&recordPath)
	bpt.payloadSize += len(newValue) - oldValueSize
	return oldValue
}

func (bpt *BPTree) findRecord(key []byte) (recordPath, bool) {
	if bpt.recordCount == 0 {
		return []recordPathComponent{{bpt.rootAddr, 0}}, false
	}

	recordPath := recordPath(make([]recordPathComponent, 0, bpt.height+1))
	nodeAddr := bpt.rootAddr

	for {
		if nodeDepth := len(recordPath) + 1; nodeDepth == bpt.height {
			leafController := bpt.getLeafController(nodeAddr)
			i, ok := leafController.LocateRecord(key, keyComparer{bpt.fileStorage})
			recordPath = append(recordPath, recordPathComponent{nodeAddr, i})
			return recordPath, ok
		}

		nonLeafController := bpt.getNonLeafController(nodeAddr)
		i, ok := nonLeafController.LocateChild(key, keyComparer{bpt.fileStorage})

		if !ok {
			i--
		}

		recordPath = append(recordPath, recordPathComponent{nodeAddr, i})
		nodeAddr = nonLeafController.GetChildAddr(i)
	}
}

func (bpt *BPTree) locateRecord(recordPath recordPath) (int64, leafController, int) {
	leafAddr := recordPath[len(recordPath)-1].NodeAddr
	leafController := bpt.getLeafController(leafAddr)
	recordIndex := recordPath[len(recordPath)-1].RecordOrNonLeafChildIndex
	return leafAddr, leafController, recordIndex
}

func (bpt *BPTree) syncKey(recordPath *recordPath) {
	n := len(*recordPath)

	if n < 2 {
		return
	}

	recordIndex := (*recordPath)[n-1].RecordOrNonLeafChildIndex

	if recordIndex >= 1 {
		return
	}

	leafController := bpt.getLeafController((*recordPath)[n-1].NodeAddr)

	for i := n - 2; i >= 0; i-- {
		nonLeafChildIndex := (*recordPath)[i].RecordOrNonLeafChildIndex

		if nonLeafChildIndex >= 1 {
			nonLeafController := bpt.getNonLeafController((*recordPath)[i].NodeAddr)
			nonLeafController.SetKey(nonLeafChildIndex, leafController.GetKey(0))
			bpt.ensureNotUnderloadNonLeaf(recordPath, i)
			bpt.ensureNotOverloadNonLeaf(recordPath, i)
			return
		}
	}
}

func (bpt *BPTree) ensureNotOverloadLeaf(recordPath *recordPath) {
	i := len(*recordPath) - 1
	leafAddr := (*recordPath)[i].NodeAddr
	leafController1 := bpt.getLeafController(leafAddr)

	if leafController1.GetLoadSize() <= leafOverloadThreshold {
		return
	}

	recordIndex := (*recordPath)[i].RecordOrNonLeafChildIndex

	if i == 0 {
		bpt.increaseHeight()
		// >>> fix node controllers begin
		leafController1 = bpt.getLeafController(leafAddr)
		// <<< fix node controllers end
		// >>> fix record path begin
		*recordPath = append(*recordPath, recordPathComponent{})
		copy((*recordPath)[1:], (*recordPath)[0:])
		(*recordPath)[0] = recordPathComponent{bpt.rootAddr, 0}
		i = 1
		// <<< fix record path end
	}

	leafParentAddr := (*recordPath)[i-1].NodeAddr
	leafParentController := bpt.getNonLeafController(leafParentAddr)
	leafIndex := (*recordPath)[i-1].RecordOrNonLeafChildIndex

	if leafIndex < leafParentController.NumberOfChildren()-1 {
		leafRSiblingAddr := leafParentController.GetChildAddr(leafIndex + 1)
		leafRSiblingController := bpt.getLeafController(leafRSiblingAddr)

		if numberOfRecords := leafController1.CountRecordsForShiftingToRight(leafRSiblingController); numberOfRecords >= 1 {
			m := leafController1.NumberOfRecords() - numberOfRecords
			leafController1.ShiftToRight(numberOfRecords, leafParentController, leafIndex, leafRSiblingController)

			if recordIndex >= m {
				// >>> fix record path begin
				(*recordPath)[i].NodeAddr = leafRSiblingAddr
				(*recordPath)[i].RecordOrNonLeafChildIndex = recordIndex - m
				(*recordPath)[i-1].RecordOrNonLeafChildIndex = leafIndex + 1
				// <<< fix record path end
			}

			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	}

	if leafIndex >= 1 {
		leafLSiblingAddr := leafParentController.GetChildAddr(leafIndex - 1)
		leafLSiblingController := bpt.getLeafController(leafLSiblingAddr)

		if numberOfRecords := leafController1.CountRecordsForShiftingToLeft(leafLSiblingController); numberOfRecords >= 1 {
			m := leafLSiblingController.NumberOfRecords()
			leafController1.ShiftToLeft(numberOfRecords, leafParentController, leafIndex, leafLSiblingController)

			if recordIndex < numberOfRecords {
				// >>> fix record path begin
				(*recordPath)[i].NodeAddr = leafLSiblingAddr
				(*recordPath)[i].RecordOrNonLeafChildIndex = m + recordIndex
				(*recordPath)[i-1].RecordOrNonLeafChildIndex = leafIndex - 1
				// <<< fix record path end
			} else {
				// >>> fix record path begin
				(*recordPath)[i].RecordOrNonLeafChildIndex = recordIndex - numberOfRecords
				// <<< fix record path end
			}

			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	}

	numberOfRecords := leafController1.CountRecordsForSpliting()
	m := leafController1.NumberOfRecords() - numberOfRecords
	leafNSiblingAddr, nodeAccessor := bpt.createLeaf()
	// >>> fix node controllers begin
	leafController1 = bpt.getLeafController(leafAddr)
	leafParentController = bpt.getNonLeafController(leafParentAddr)
	// <<< fix node controllers end
	leafNSiblingController := leafController(nodeAccessor)
	leafController1.Split(numberOfRecords, leafParentController, leafIndex, leafNSiblingController, leafNSiblingAddr)
	bpt.leafList.InsertLeafAfter(bpt.fileStorage, leafNSiblingAddr, leafAddr)

	if recordIndex >= m {
		// >>> fix record path begin
		(*recordPath)[i].NodeAddr = leafNSiblingAddr
		(*recordPath)[i].RecordOrNonLeafChildIndex = recordIndex - m
		(*recordPath)[i-1].RecordOrNonLeafChildIndex = leafIndex + 1
		// <<< fix record path end
	}

	bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
}

func (bpt *BPTree) ensureNotOverloadNonLeaf(recordPath *recordPath, i int) {
	nonLeafAddr := (*recordPath)[i].NodeAddr
	nonLeafController1 := bpt.getNonLeafController(nonLeafAddr)

	if nonLeafController1.GetLoadSize() <= nonLeafOverloadThreshold {
		return
	}

	nonLeafChildIndex := (*recordPath)[i].RecordOrNonLeafChildIndex

	if i == 0 {
		bpt.increaseHeight()
		// >>> fix node controllers begin
		nonLeafController1 = bpt.getNonLeafController(nonLeafAddr)
		// <<< fix node controllers end
		// >>> fix record path begin
		*recordPath = append(*recordPath, recordPathComponent{})
		copy((*recordPath)[1:], (*recordPath)[0:])
		(*recordPath)[0] = recordPathComponent{bpt.rootAddr, 0}
		i = 1
		// <<< fix record path end
	}

	nonLeafParentAddr := (*recordPath)[i-1].NodeAddr
	nonLeafParentController := bpt.getNonLeafController(nonLeafParentAddr)
	nonLeafIndex := (*recordPath)[i-1].RecordOrNonLeafChildIndex

	if nonLeafIndex < nonLeafParentController.NumberOfChildren()-1 {
		nonLeafRSiblingAddr := nonLeafParentController.GetChildAddr(nonLeafIndex + 1)
		nonLeafRSiblingController := bpt.getNonLeafController(nonLeafRSiblingAddr)

		if numberOfChildren := nonLeafController1.CountChildrenForShiftingToRight(nonLeafRSiblingController); numberOfChildren >= 1 {
			m := nonLeafController1.NumberOfChildren() - numberOfChildren
			nonLeafController1.ShiftToRight(numberOfChildren, nonLeafParentController, nonLeafIndex, nonLeafRSiblingController)

			if nonLeafChildIndex >= m {
				// >>> fix record path begin
				(*recordPath)[i].NodeAddr = nonLeafRSiblingAddr
				(*recordPath)[i].RecordOrNonLeafChildIndex = nonLeafChildIndex - m
				(*recordPath)[i-1].RecordOrNonLeafChildIndex = nonLeafIndex + 1
				// <<< fix record path end
			}

			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	}

	if nonLeafIndex >= 1 {
		nonLeafLSiblingAddr := nonLeafParentController.GetChildAddr(nonLeafIndex - 1)
		nonLeafLSiblingController := bpt.getNonLeafController(nonLeafLSiblingAddr)

		if numberOfChildren := nonLeafController1.CountChildrenForShiftingToLeft(nonLeafLSiblingController); numberOfChildren >= 1 {
			m := nonLeafLSiblingController.NumberOfChildren()
			nonLeafController1.ShiftToLeft(numberOfChildren, nonLeafParentController, nonLeafIndex, nonLeafLSiblingController)

			if nonLeafChildIndex < numberOfChildren {
				// >>> fix record path begin
				(*recordPath)[i].NodeAddr = nonLeafLSiblingAddr
				(*recordPath)[i].RecordOrNonLeafChildIndex = m + nonLeafChildIndex
				(*recordPath)[i-1].RecordOrNonLeafChildIndex = nonLeafIndex - 1
				// <<< fix record path end
			} else {
				// >>> fix record path begin
				(*recordPath)[i].RecordOrNonLeafChildIndex = nonLeafChildIndex - numberOfChildren
				// <<< fix record path end
			}

			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	}

	numberOfChildren := nonLeafController1.CountChildrenForSpliting()
	m := nonLeafController1.NumberOfChildren() - numberOfChildren
	nonLeafNSiblingAddr, nodeAccessor := bpt.createNonLeaf()
	// >>> fix node controllers begin
	nonLeafController1 = bpt.getNonLeafController(nonLeafAddr)
	nonLeafParentController = bpt.getNonLeafController(nonLeafParentAddr)
	// <<< fix node controllers ned
	nonLeafNSiblingController := nonLeafController(nodeAccessor)
	nonLeafController1.Split(numberOfChildren, nonLeafParentController, nonLeafIndex, nonLeafNSiblingController, nonLeafNSiblingAddr)

	if nonLeafChildIndex >= m {
		// >>> fix record path begin
		(*recordPath)[i].NodeAddr = nonLeafNSiblingAddr
		(*recordPath)[i].RecordOrNonLeafChildIndex = nonLeafChildIndex - m
		(*recordPath)[i-1].RecordOrNonLeafChildIndex = nonLeafIndex + 1
		// <<< fix record path end
	}

	bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
}

func (bpt *BPTree) ensureNotUnderloadLeaf(recordPath *recordPath) {
	i := len(*recordPath) - 1

	if i == 0 {
		return
	}

	leafAddr := (*recordPath)[i].NodeAddr
	leafController1 := bpt.getLeafController(leafAddr)

	if leafController1.GetLoadSize() >= leafUnderloadThreshold {
		return
	}

	recordIndex := (*recordPath)[i].RecordOrNonLeafChildIndex
	leafParentAddr := (*recordPath)[i-1].NodeAddr
	leafParentController := bpt.getNonLeafController(leafParentAddr)
	leafIndex := (*recordPath)[i-1].RecordOrNonLeafChildIndex
	var leafRSiblingAddr, leafLSiblingAddr int64
	var leafRSiblingController, leafLSiblingController leafController

	if leafIndex < leafParentController.NumberOfChildren()-1 {
		leafRSiblingAddr = leafParentController.GetChildAddr(leafIndex + 1)
		leafRSiblingController = bpt.getLeafController(leafRSiblingAddr)

		if numberOfRecords := leafController1.CountRecordsForUnshiftingFromRight(leafRSiblingController); numberOfRecords >= 1 {
			leafController1.UnshiftFromRight(numberOfRecords, leafParentController, leafIndex, leafRSiblingController)
			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	} else {
		leafRSiblingAddr = -1
	}

	if leafIndex >= 1 {
		leafLSiblingAddr = leafParentController.GetChildAddr(leafIndex - 1)
		leafLSiblingController = bpt.getLeafController(leafLSiblingAddr)

		if numberOfRecords := leafController1.CountRecordsForUnshiftingFromLeft(leafLSiblingController); numberOfRecords >= 1 {
			leafController1.UnshiftFromLeft(numberOfRecords, leafParentController, leafIndex, leafLSiblingController)
			// >>> fix record path begin
			(*recordPath)[i].RecordOrNonLeafChildIndex = numberOfRecords + recordIndex
			// <<< fix record path end
			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	} else {
		leafLSiblingAddr = -1
	}

	if leafRSiblingAddr >= 0 {
		bpt.leafList.RemoveLeaf(bpt.fileStorage, leafRSiblingAddr)
		leafController1.MergeFromRight(leafParentController, leafIndex, leafRSiblingController)
		bpt.destroyLeaf(leafRSiblingAddr)
	} else {
		bpt.leafList.RemoveLeaf(bpt.fileStorage, leafAddr)
		m := leafLSiblingController.NumberOfRecords()
		leafController1.MergeToLeft(leafParentController, leafIndex, leafLSiblingController)
		bpt.destroyLeaf(leafAddr)
		// >>> fix record path begin
		(*recordPath)[i].NodeAddr = leafLSiblingAddr
		(*recordPath)[i].RecordOrNonLeafChildIndex = m + recordIndex
		(*recordPath)[i-1].RecordOrNonLeafChildIndex = leafIndex - 1
		// <<< fix record path end
	}

	bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
}

func (bpt *BPTree) ensureNotUnderloadNonLeaf(recordPath *recordPath, i int) {
	nonLeafAddr := (*recordPath)[i].NodeAddr
	nonLeafController1 := bpt.getNonLeafController(nonLeafAddr)

	if i == 0 {
		if nonLeafController1.NumberOfChildren() == 1 {
			bpt.decreaseHeight()
			// >>> fix record path begin
			copy((*recordPath)[0:], (*recordPath)[1:])
			*recordPath = (*recordPath)[:len(*recordPath)-1]
			// <<< fix record path end
		}

		return
	}

	if nonLeafController1.GetLoadSize() >= nonLeafUnderloadThreshold {
		return
	}

	nonLeafChildIndex := (*recordPath)[i].RecordOrNonLeafChildIndex
	nonLeafParentAddr := (*recordPath)[i-1].NodeAddr
	nonLeafParentController := bpt.getNonLeafController(nonLeafParentAddr)
	nonLeafIndex := (*recordPath)[i-1].RecordOrNonLeafChildIndex
	var nonLeafRSiblingAddr, nonLeafLSiblingAddr int64
	var nonLeafRSiblingController, nonLeafLSiblingController nonLeafController

	if nonLeafIndex < nonLeafParentController.NumberOfChildren()-1 {
		nonLeafRSiblingAddr = nonLeafParentController.GetChildAddr(nonLeafIndex + 1)
		nonLeafRSiblingController = bpt.getNonLeafController(nonLeafRSiblingAddr)

		if numberOfChildren := nonLeafController1.CountChildrenForUnshiftingFromRight(nonLeafRSiblingController); numberOfChildren >= 1 {
			nonLeafController1.UnshiftFromRight(numberOfChildren, nonLeafParentController, nonLeafIndex, nonLeafRSiblingController)
			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	} else {
		nonLeafRSiblingAddr = -1
	}

	if nonLeafIndex >= 1 {
		nonLeafLSiblingAddr = nonLeafParentController.GetChildAddr(nonLeafIndex - 1)
		nonLeafLSiblingController = bpt.getNonLeafController(nonLeafLSiblingAddr)

		if numberOfChildren := nonLeafController1.CountChildrenForUnshiftingFromLeft(nonLeafLSiblingController); numberOfChildren >= 1 {
			nonLeafController1.UnshiftFromLeft(numberOfChildren, nonLeafParentController, nonLeafIndex, nonLeafLSiblingController)
			// >>> fix record path begin
			(*recordPath)[i].RecordOrNonLeafChildIndex = numberOfChildren + nonLeafChildIndex
			// <<< fix record path end
			bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
			bpt.ensureNotOverloadNonLeaf(recordPath, i-1)
			return
		}
	} else {
		nonLeafLSiblingAddr = -1
	}

	if nonLeafRSiblingAddr >= 0 {
		nonLeafController1.MergeFromRight(nonLeafParentController, nonLeafIndex, nonLeafRSiblingController)
		bpt.destroyNonLeaf(nonLeafRSiblingAddr)
	} else {
		m := nonLeafLSiblingController.NumberOfChildren()
		nonLeafController1.MergeToLeft(nonLeafParentController, nonLeafIndex, nonLeafLSiblingController)
		bpt.destroyNonLeaf(nonLeafAddr)
		// >>> fix record path begin
		(*recordPath)[i].NodeAddr = nonLeafLSiblingAddr
		(*recordPath)[i].RecordOrNonLeafChildIndex = m + nonLeafChildIndex
		(*recordPath)[i-1].RecordOrNonLeafChildIndex = nonLeafIndex - 1
		// <<< fix record path end
	}

	bpt.ensureNotUnderloadNonLeaf(recordPath, i-1)
}

func (bpt *BPTree) increaseHeight() {
	rootAddr, rootController := bpt.createNonLeaf()
	rootController.InsertChildren(0, []nonLeafChild{{nil, bpt.rootAddr}})
	bpt.rootAddr = rootAddr
	bpt.height++
}

func (bpt *BPTree) decreaseHeight() {
	rootController := bpt.getNonLeafController(bpt.rootAddr)
	rootAddr := rootController.GetChildAddr(0)
	bpt.destroyNonLeaf(bpt.rootAddr)
	bpt.rootAddr = rootAddr
	bpt.height--
}

func (bpt *BPTree) search(minKey []byte, maxKey []byte) (int64, int, int64, int, bool) {
	if bpt.recordCount == 0 {
		return 0, 0, 0, 0, false
	}

	f1 := isMinKey(minKey)
	f2 := isMinKey(maxKey)
	f3 := isMaxKey(minKey)
	f4 := isMaxKey(maxKey)
	ok1 := f1 || f3
	ok2 := f2 || f4
	var d int

	if ok1 || ok2 {
		if ok1 && ok2 {
			if bpt.recordCount == 1 {
				d = 0
			} else {
				if f1 == f2 {
					d = 0
				} else {
					if !f1 {
						return 0, 0, 0, 0, false
					}

					d = -1
				}
			}
		} else {
			d = -1
		}
	} else {
		d = bytes.Compare(minKey, maxKey)

		if d > 0 {
			return 0, 0, 0, 0, false
		}
	}

	minRecordPath, ok3 := bpt.findRecord(minKey)
	minLeafAddr, minLeafController, minRecordIndex := bpt.locateRecord(minRecordPath)

	if !ok3 {
		if minRecordIndex == minLeafController.NumberOfRecords() {
			if minLeafAddr == bpt.leafList.TailAddr() {
				return 0, 0, 0, 0, false
			}

			minLeafAddr = leafHeader(minLeafController).NextAddr()
			minRecordIndex = 0
		}
	}

	if d == 0 {
		return minLeafAddr, minRecordIndex, minLeafAddr, minRecordIndex, true
	}

	if !(!ok1 && ok3) {
		minKey = keyFactory{bpt.fileStorage}.ReadKey(minLeafController.GetKey(minRecordIndex))

		if !ok2 {
			d = bytes.Compare(minKey, maxKey)

			if d > 0 {
				return 0, 0, 0, 0, false
			}

			if d == 0 {
				return minLeafAddr, minRecordIndex, minLeafAddr, minRecordIndex, true
			}
		}
	}

	maxRecordPath, ok4 := bpt.findRecord(maxKey)
	maxLeafAddr, maxLeafController, maxRecordIndex := bpt.locateRecord(maxRecordPath)

	if !ok4 {
		maxRecordIndex--
	}

	if !(!ok2 && ok4) {
		maxKey = keyFactory{bpt.fileStorage}.ReadKey(maxLeafController.GetKey(maxRecordIndex))
		d = bytes.Compare(minKey, maxKey)

		if d > 0 {
			return 0, 0, 0, 0, false
		}
	}

	return minLeafAddr, minRecordIndex, maxLeafAddr, maxRecordIndex, true
}

func (bpt *BPTree) createLeaf() (int64, leafController) {
	leafAddr, leafController := leafFactory{bpt.fileStorage}.CreateLeaf()
	bpt.leafCount++
	return leafAddr, leafController
}

func (bpt *BPTree) destroyLeaf(leafAddr int64) {
	leafFactory{bpt.fileStorage}.DestroyLeaf(leafAddr)
	bpt.leafCount--
}

func (bpt *BPTree) getLeafController(leafAddr int64) leafController {
	return leafFactory{bpt.fileStorage}.GetLeafController(leafAddr)
}

func (bpt *BPTree) createNonLeaf() (int64, nonLeafController) {
	nonLeafAddr, nonLeafController := nonLeafFactory{bpt.fileStorage}.CreateNonLeaf()
	bpt.nonLeafCount++
	return nonLeafAddr, nonLeafController
}

func (bpt *BPTree) destroyNonLeaf(nonLeafAddr int64) {
	nonLeafFactory{bpt.fileStorage}.DestroyNonLeaf(nonLeafAddr)
	bpt.nonLeafCount--
}

func (bpt *BPTree) getNonLeafController(nonLeafAddr int64) nonLeafController {
	return nonLeafFactory{bpt.fileStorage}.GetNonLeafController(nonLeafAddr)
}

type recordPath []recordPathComponent

type recordPathComponent struct {
	NodeAddr                  int64
	RecordOrNonLeafChildIndex int
}
