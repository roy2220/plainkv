package plainkv

import (
	"github.com/roy2220/fsm"
	"github.com/roy2220/plainkv/bptree"
)

// OrderedDict represents an ordered dictionary.
type OrderedDict struct {
	fileStorage fsm.FileStorage
	bpTree      bptree.BPTree
}

// OpenOrderedDict opens an ordered dictionary on the given file.
func OpenOrderedDict(fileName string, createFileIfNotExists bool) (*OrderedDict, error) {
	var od OrderedDict
	od.fileStorage.Init()

	if err := od.fileStorage.Open(fileName, createFileIfNotExists); err != nil {
		return nil, err
	}

	od.bpTree.Init(&od.fileStorage)

	if bpTreeInfoAddr := od.fileStorage.PrimarySpace(); bpTreeInfoAddr < 0 {
		od.bpTree.Create()
	} else {
		od.bpTree.Load(bpTreeInfoAddr)
	}

	return &od, nil
}

// Close closes the dictionary.
func (od *OrderedDict) Close() error {
	bpTreeInfoAddr := od.bpTree.Store()
	od.fileStorage.SetPrimarySpace(bpTreeInfoAddr)
	return od.fileStorage.Close()
}

// Set sets the value for the given key in the dictionary to the
// given value.
// If the key already exists it replaces the value and then
// returns the replaced value (optional).
func (od *OrderedDict) Set(key []byte, value []byte, returnReplacedValue bool) []byte {
	value, _ = od.bpTree.AddOrUpdateRecord(key, value, returnReplacedValue)
	return value
}

// SetIfExists sets the value for the given key in the dictionary
// to the given value.
// If the key exists, it replaces the value and then returns true
// and the replaced value (optional), otherwise it returns false.
func (od *OrderedDict) SetIfExists(key []byte, value []byte, returnReplacedValue bool) ([]byte, bool) {
	return od.bpTree.UpdateRecord(key, value, returnReplacedValue)
}

// SetIfNotExists sets the value for the given key in the
// dictionary to the given value.
// If the key doesn't exists, it adds the key with the value and
// then returns true, otherwise it returns false and the present
// value (optional).
func (od *OrderedDict) SetIfNotExists(key []byte, value []byte, returnPresentValue bool) ([]byte, bool) {
	return od.bpTree.AddRecord(key, value, returnPresentValue)
}

// Clear clears the given key in the dictionary.
// If the key exists, it deletes the key and then returns true
// and the removed value (optional), otherwise if returns false.
func (od *OrderedDict) Clear(key []byte, returnRemovedValue bool) ([]byte, bool) {
	return od.bpTree.DeleteRecord(key, returnRemovedValue)
}

// Test tests the given key in the dictionary.
// If the key exists, it returns true and the present value (optional),
// otherwise it returns false.
func (od *OrderedDict) Test(key []byte, returnPresentValue bool) ([]byte, bool) {
	return od.bpTree.HasRecord(key, returnPresentValue)
}

// RangeAsc looks up the the dictionary for keys in the given range
// [minKey...maxKey] and keys' values.
// It returns an iterator to iterate over the keys/values found
// in ascending order.
func (od *OrderedDict) RangeAsc(minKey []byte, maxKey []byte) OrderedDictIterator {
	return od.bpTree.SearchForward(minKey, maxKey)
}

// RangeDesc looks up the the dictionary for keys in the given range
// [minKey...maxKey] and keys' values.
// It returns an iterator to iterate over the keys/values found
// in descending order.
func (od *OrderedDict) RangeDesc(minKey []byte, maxKey []byte) OrderedDictIterator {
	return od.bpTree.SearchBackward(minKey, maxKey)
}

// Stats returns the stats of the dictionary.
func (od *OrderedDict) Stats() OrderedDictStats {
	return OrderedDictStats{
		FSM:                    od.fileStorage.Stats(),
		BPTreeHeight:           od.bpTree.Height(),
		NumberOfBPTreeLeafs:    od.bpTree.NumberOfLeafs(),
		NumberOfBPTreeNonLeafs: od.bpTree.NumberOfNonLeafs(),
		NumberOfBPTreeRecords:  od.bpTree.NumberOfRecords(),
		PayloadSize:            od.bpTree.PayloadSize(),
	}
}

// OrderedDictStats represents the stats of an ordered dictionary
type OrderedDictStats struct {
	FSM                    fsm.Stats
	BPTreeHeight           int
	NumberOfBPTreeLeafs    int
	NumberOfBPTreeNonLeafs int
	NumberOfBPTreeRecords  int
	PayloadSize            int
}

// OrderedDictIterator represents an iteration over keys/values in an ordered dictionary.
type OrderedDictIterator = bptree.Iterator

var (
	// MinKey presents the minimum key in an ordered dictionary.
	MinKey = bptree.MinKey

	// MaxKey presents the maximum key in an ordered dictionary.
	MaxKey = bptree.MaxKey
)
