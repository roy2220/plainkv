// Package plainkv implements a key/value storage.
package plainkv

import (
	"github.com/roy2220/fsm"
	"github.com/roy2220/plainkv/hashmap"
)

// Dict represents a dictionary.
type Dict struct {
	fileStorage fsm.FileStorage
	hashMap     hashmap.HashMap
}

// OpenDict opens a dictionary on the given file.
func OpenDict(fileName string, createFileIfNotExists bool) (*Dict, error) {
	var d Dict
	d.fileStorage.Init()

	if err := d.fileStorage.Open(fileName, createFileIfNotExists); err != nil {
		return nil, err
	}

	d.hashMap.Init(&d.fileStorage)

	if hashMapInfoAddr := d.fileStorage.PrimarySpace(); hashMapInfoAddr < 0 {
		d.hashMap.Create()
	} else {
		d.hashMap.Load(hashMapInfoAddr)
	}

	return &d, nil
}

// Close closes the dictionary.
func (d *Dict) Close() error {
	hashMapInfoAddr := d.hashMap.Store()
	d.fileStorage.SetPrimarySpace(hashMapInfoAddr)
	return d.fileStorage.Close()
}

// Set sets the value for the given key in the dictionary to the
// given value.
// If the key already exists it replaces the value and then
// returns the replaced value (optional).
func (d *Dict) Set(key []byte, value []byte, returnReplacedValue bool) []byte {
	value, _ = d.hashMap.AddOrUpdateItem(key, value, returnReplacedValue)
	return value
}

// SetIfExists sets the value for the given key in the dictionary
// to the given value.
// If the key exists, it replaces the value and then returns true
// and the replaced value (optional), otherwise it returns false.
func (d *Dict) SetIfExists(key []byte, value []byte, returnReplacedValue bool) ([]byte, bool) {
	return d.hashMap.UpdateItem(key, value, returnReplacedValue)
}

// SetIfNotExists sets the value for the given key in the
// dictionary to the given value.
// If the key doesn't exists, it adds the key with the value and
// then returns true, otherwise it returns false and the present
// value (optional).
func (d *Dict) SetIfNotExists(key []byte, value []byte, returnPresentValue bool) ([]byte, bool) {
	return d.hashMap.AddItem(key, value, returnPresentValue)
}

// Clear clears the given key in the dictionary.
// If the key exists, it deletes the key and then returns true
// and the removed value (optional), otherwise if returns false.
func (d *Dict) Clear(key []byte, returnRemovedValue bool) ([]byte, bool) {
	return d.hashMap.DeleteItem(key, returnRemovedValue)
}

// Test tests the given key in the dictionary.
// If the key exists, it returns true and the present value (optional),
// otherwise it returns false.
func (d *Dict) Test(key []byte, returnPresentValue bool) ([]byte, bool) {
	return d.hashMap.HasItem(key, returnPresentValue)
}

// Scan scans the dictionary for a key and value from the given
// cursor, and meanwhile advances the given cursor to the next
// position.
// It returns false if there are no more keys and values.
// The initial cursor is of the zero value.
func (d *Dict) Scan(cursor *DictCursor) ([]byte, []byte, bool) {
	return d.hashMap.FetchItem(cursor)
}

// Stats returns the stats of the dictionary.
func (d *Dict) Stats() Stats {
	return Stats{
		FSM:                  d.fileStorage.Stats(),
		NumberOfHashSlotDirs: d.hashMap.NumberOfSlotDirs(),
		NumberOfHashSlots:    d.hashMap.NumberOfSlots(),
		NumberOfHashItems:    d.hashMap.NumberOfItems(),
		PayloadSize:          d.hashMap.PayloadSize(),
	}
}

// DictCursor represents a cursor at a position in a dictionary.
type DictCursor = hashmap.Cursor

// Stats represents the stats about key/value storages.
type Stats struct {
	FSM                  fsm.Stats
	NumberOfHashSlotDirs int
	NumberOfHashSlots    int
	NumberOfHashItems    int
	PayloadSize          int
}
