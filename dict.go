// Package plainkv implements a key/value storage.
package plainkv

import (
	"github.com/roy2220/fsm"
	"github.com/roy2220/plainkv/internal/hashmap"
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
// If the key already exists it updates the value then returns
// the original value.
func (d *Dict) Set(key []byte, value []byte) []byte {
	value, _ = d.hashMap.AddOrUpdateItem(key, value)
	return value
}

// SetIfExists sets the value for the given key in the dictionary
// to the given value.
// If the key exists, it updates the value then returns true and
// the original value, otherwise it returns false.
func (d *Dict) SetIfExists(key []byte, value []byte) ([]byte, bool) {
	return d.hashMap.UpdateItem(key, value)
}

// SetIfNotExists sets the value for the given key in the
// dictionary to the given value.
// If the key doesn't exists, it adds the key with the value then
// returns true, otherwise it returns false and the value for the
// existing key.
func (d *Dict) SetIfNotExists(key []byte, value []byte) ([]byte, bool) {
	return d.hashMap.AddItem(key, value)
}

// Clear clears the value for the given key in the dictionary.
// If the key exists, it deletes the key then returns true and the
// value, otherwise if returns false.
func (d *Dict) Clear(key []byte) ([]byte, bool) {
	return d.hashMap.DeleteItem(key)
}

// Get gets the value for the given key in the dictionary.
// If the key exists, it returns true and the value, otherwise
// it returns false.
func (d *Dict) Get(key []byte) ([]byte, bool) {
	return d.hashMap.HasItem(key)
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
