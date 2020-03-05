// Package hashmap implements a hash map.
package hashmap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"

	"github.com/gogo/protobuf/proto"
	"github.com/roy2220/fsm"

	"github.com/roy2220/plainkv/internal/protocol"
)

// HashMap represents a hash map.
type HashMap struct {
	fileStorage          *fsm.FileStorage
	slotDirsAddr         int64
	maxSlotDirCountShift int
	slotDirCount         int
	minSlotCountShift    int
	slotCount            int
	itemCount            int
	payloadSize          int
}

// Init initializes the hash map with a given file storage and returns it.
func (hm *HashMap) Init(fileStorage *fsm.FileStorage) *HashMap {
	hm.fileStorage = fileStorage
	hm.slotDirsAddr = -1
	return hm
}

// Create creates the hash map on the file storage.
func (hm *HashMap) Create() {
	slotDirsAddr, buffer1 := hm.fileStorage.AllocateSpace(8 << minMaxSlotDirCountShift)
	slotDirAddr, buffer2 := hm.fileStorage.AllocateSpace(8 << slotDirLengthShift)
	binary.BigEndian.PutUint64(buffer1, uint64(slotDirAddr))
	binary.BigEndian.PutUint64(buffer2, ^uint64(0))
	hm.slotDirsAddr = slotDirsAddr
	hm.maxSlotDirCountShift = minMaxSlotDirCountShift
	hm.slotDirCount = 1
	hm.slotCount = 1
}

// Destroy destroys the hash map on the file storage.
func (hm *HashMap) Destroy() {
	slotDirAddr := int64(binary.BigEndian.Uint64(hm.fileStorage.AccessSpace(hm.slotDirsAddr)))
	hm.fileStorage.FreeSpace(hm.slotDirsAddr)
	hm.fileStorage.FreeSpace(slotDirAddr)
	*hm = *new(HashMap).Init(hm.fileStorage)
}

// Load loads the hash map from the file storage with the
// given info address.
func (hm *HashMap) Load(infoAddr int64) {
	buffer := proto.NewBuffer(hm.fileStorage.AccessSpace(infoAddr))
	var info protocol.HashMapInfo

	if err := buffer.DecodeMessage(&info); err != nil {
		panic(errCorrupted)
	}

	hm.fileStorage.FreeSpace(infoAddr)
	hm.slotDirsAddr = info.SlotDirsAddr
	hm.maxSlotDirCountShift = int(info.MaxSlotDirCountShift)
	hm.slotDirCount = int(info.SlotDirCount)
	hm.minSlotCountShift = int(info.MinSlotCountShift)
	hm.slotCount = int(info.SlotCount)
	hm.itemCount = int(info.ItemCount)
	hm.payloadSize = int(info.PayloadSize)
}

// Store stores the hash map to the file storage then returns
// the info address.
func (hm *HashMap) Store() int64 {
	buffer := proto.NewBuffer(nil)

	buffer.EncodeMessage(&protocol.HashMapInfo{
		SlotDirsAddr:         hm.slotDirsAddr,
		MaxSlotDirCountShift: int64(hm.maxSlotDirCountShift),
		SlotDirCount:         int64(hm.slotDirCount),
		MinSlotCountShift:    int64(hm.minSlotCountShift),
		SlotCount:            int64(hm.slotCount),
		ItemCount:            int64(hm.itemCount),
		PayloadSize:          int64(hm.payloadSize),
	})

	infoAddr, buffer2 := hm.fileStorage.AllocateSpace(len(buffer.Bytes()))
	copy(buffer2, buffer.Bytes())
	*hm = *new(HashMap).Init(hm.fileStorage)
	return infoAddr
}

// AddItem adds the given item to the hash map.
// If no item matched exists in the hash map, it adds the item
// then returns true, otherwise it returns false and the value
// of the item.
func (hm *HashMap) AddItem(key []byte, value []byte) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := hm.loadSlot(slotAddr)

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			return item.Value, false
		}
	}

	hm.payloadSize += len(key) + len(value)

	if len(key) <= maxShortKeySize {
		keySum = 0
	}

	items = append(items, protocol.HashItem{
		KeySum: keySum,
		Key:    key,
		Value:  value,
	})

	hm.restoreSlot(&slotAddr, items)
	slotAddrRef.Set(hm.fileStorage, slotAddr)
	hm.maybeExpand()
	return nil, true
}

// UpdateItem replaces the value of an item with the given key
// in the hash map to the given one.
// If an item matched exists in the hash map, it updates the item
// then returns true and the original value of the item, otherwise
// it returns false.
func (hm *HashMap) UpdateItem(key []byte, value []byte) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := hm.loadSlot(slotAddr)

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			hm.payloadSize += len(value) - len(item.Value)
			value, item.Value = item.Value, value
			hm.restoreSlot(&slotAddr, items)
			slotAddrRef.Set(hm.fileStorage, slotAddr)
			return value, true
		}
	}

	return nil, false
}

// AddOrUpdateItem adds the given item to the hash map or replaces
// the value of an item with the given key to the given one.
// If no item matched exists in the hash map, it adds the item then
// returns true, otherwise it updates the item then returns false
// and the original value of the item.
func (hm *HashMap) AddOrUpdateItem(key []byte, value []byte) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := hm.loadSlot(slotAddr)

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			hm.payloadSize += len(value) - len(item.Value)
			value, item.Value = item.Value, value
			hm.restoreSlot(&slotAddr, items)
			slotAddrRef.Set(hm.fileStorage, slotAddr)
			return value, false
		}
	}

	hm.payloadSize += len(key) + len(value)

	if len(key) <= maxShortKeySize {
		keySum = 0
	}

	items = append(items, protocol.HashItem{
		KeySum: keySum,
		Key:    key,
		Value:  value,
	})

	hm.restoreSlot(&slotAddr, items)
	slotAddrRef.Set(hm.fileStorage, slotAddr)
	hm.maybeExpand()
	return nil, true
}

// DeleteItem deletes an item with the given key in the hash map.
// If an item matched exists in the hash map, it deletes the item
// then returns true and the value of the item, otherwise it
// returns false.
func (hm *HashMap) DeleteItem(key []byte) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := hm.loadSlot(slotAddr)

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			hm.payloadSize -= len(item.Key) + len(item.Value)
			value := item.Value
			n := len(items)

			for j := i + 1; j < n; j++ {
				items[j-1] = items[j]
			}

			items = items[:n-1]
			hm.restoreSlot(&slotAddr, items)
			slotAddrRef.Set(hm.fileStorage, slotAddr)
			hm.maybeShrink()
			return value, true
		}
	}

	return nil, false
}

// HasItem returns the value of an item with the given key in
// the hash map.
// If an item matched exists in the hash map, it returns true
// and the value of the item, otherwise it returns false.
func (hm *HashMap) HasItem(key []byte) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddr := hm.locateSlotAddr(hm.calculateSlotIndex(keySum)).Get(hm.fileStorage)
	items := hm.loadSlot(slotAddr)

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			return item.Value, true
		}
	}

	return nil, false
}

// FetchItem fetches an item from the given cursor in the hash map,
// and meanwhile advances the given cursor to the next position.
// It returns false if there are no more items.
// The initial cursor is of the zero value.
func (hm *HashMap) FetchItem(cursor *Cursor) ([]byte, []byte, bool) {
	if cursor.itemIndex < len(cursor.items) {
		item := &cursor.items[cursor.itemIndex]
		cursor.itemIndex++
		return item.Key, item.Value, true
	}

	for cursor.slotIndex < hm.slotCount {
		cursor.items = hm.loadSlot(hm.locateSlotAddr(cursor.slotIndex).Get(hm.fileStorage))
		cursor.slotIndex++

		if len(cursor.items) >= 1 {
			item := &cursor.items[0]
			cursor.itemIndex = 1
			return item.Key, item.Value, true
		}
	}

	return nil, nil, false
}

// MaxNumberOfSlotDirs returns the maximum number of the slot
// directories of the hash map.
func (hm *HashMap) MaxNumberOfSlotDirs() int {
	return 1 << hm.maxSlotDirCountShift
}

// NumberOfSlotDirs returns the number of the slot directories
// of the hash map.
func (hm *HashMap) NumberOfSlotDirs() int {
	return hm.slotDirCount
}

// MinNumberOfSlots returns the minimum number of the slots of
// the hash map.
func (hm *HashMap) MinNumberOfSlots() int {
	return 1 << hm.minSlotCountShift
}

// NumberOfSlots returns the number of the slots of the hash map.
func (hm *HashMap) NumberOfSlots() int {
	return hm.slotCount
}

// NumberOfItems returns the number of the items of the hash map.
func (hm *HashMap) NumberOfItems() int {
	return hm.itemCount
}

// PayloadSize returns the payload size of the hash map.
func (hm *HashMap) PayloadSize() int {
	return hm.payloadSize
}

func (hm *HashMap) calculateSlotIndex(keySum uint64) int {
	slotIndex := int(keySum & ((1 << (hm.minSlotCountShift + 1)) - 1))

	if slotIndex >= hm.slotCount {
		slotIndex = hm.calculateParentSlotIndex(slotIndex)
	}

	return slotIndex
}

func (hm *HashMap) calculateParentSlotIndex(slotIndex int) int {
	return slotIndex &^ (1 << hm.minSlotCountShift)
}

func (hm *HashMap) locateSlotAddr(slotIndex int) addrRef {
	return addrRef{
		Addr:  hm.locateSlotDirAddr(slotIndex >> slotDirLengthShift).Get(hm.fileStorage),
		Index: slotIndex & ((1 << slotDirLengthShift) - 1),
	}
}

func (hm *HashMap) locateSlotDirAddr(slotDirIndex int) addrRef {
	return addrRef{
		Addr:  hm.slotDirsAddr,
		Index: slotDirIndex,
	}
}

func (hm *HashMap) storeSlot(items []protocol.HashItem) int64 {
	if len(items) == 0 {
		return -1
	}

	slot := protocol.HashSlot{Items: items}
	slotSize := slot.Size()
	var rawSlotSize [binary.MaxVarintLen64]byte
	i := binary.PutUvarint(rawSlotSize[:], uint64(slotSize))
	slotAddr, buffer := hm.fileStorage.AllocateSpace(i + slotSize)
	copy(buffer, rawSlotSize[:i])
	slot.MarshalTo(buffer[i:])
	return slotAddr
}

func (hm *HashMap) eraseSlot(slotAddr int64) {
	if slotAddr < 0 {
		return
	}

	hm.fileStorage.FreeSpace(slotAddr)
}

func (hm *HashMap) restoreSlot(slotAddr *int64, items []protocol.HashItem) {
	hm.eraseSlot(*slotAddr)
	*slotAddr = hm.storeSlot(items)
}

func (hm *HashMap) loadSlot(slotAddr int64) []protocol.HashItem {
	if slotAddr < 0 {
		return nil
	}

	buffer := hm.fileStorage.AccessSpace(slotAddr)
	n, i := binary.Uvarint(buffer)

	if i <= 0 {
		panic(errCorrupted)
	}

	slotSize := int(n)
	var slot protocol.HashSlot

	if err := slot.Unmarshal(buffer[i : i+slotSize]); err != nil {
		panic(errCorrupted)
	}

	return slot.Items
}

func (hm *HashMap) maybeExpand() {
	hm.itemCount++

	for float64(hm.itemCount)/float64(hm.slotCount) > loadFactor {
		slotIndex := hm.calculateParentSlotIndex(hm.slotCount)
		slotAddrRef := hm.locateSlotAddr(slotIndex)
		slotAddr := slotAddrRef.Get(hm.fileStorage)
		items := hm.loadSlot(slotAddr)
		items1, items2 := splitItems(items, uint64(1<<hm.minSlotCountShift))
		hm.restoreSlot(&slotAddr, items1)
		slotAddrRef.Set(hm.fileStorage, slotAddr)
		hm.addSlot(items2)
	}
}

func (hm *HashMap) maybeShrink() {
	hm.itemCount--

	for hm.slotCount >= 2 && float64(hm.itemCount)/float64(hm.slotCount) <= loadFactor*loadFactor {
		items2 := hm.removeSlot()
		slotIndex := hm.calculateParentSlotIndex(hm.slotCount)
		slotAddrRef := hm.locateSlotAddr(slotIndex)
		slotAddr := slotAddrRef.Get(hm.fileStorage)
		items1 := hm.loadSlot(slotAddr)
		items := mergeItems(items1, items2)
		hm.restoreSlot(&slotAddr, items)
		slotAddrRef.Set(hm.fileStorage, slotAddr)
	}
}

func (hm *HashMap) addSlot(items []protocol.HashItem) {
	if hm.slotCount == hm.slotDirCount<<slotDirLengthShift {
		hm.addSlotDir()
	}

	slotAddr := hm.storeSlot(items)
	hm.locateSlotAddr(hm.slotCount).Set(hm.fileStorage, slotAddr)
	hm.slotCount++

	if hm.slotCount == 1<<(hm.minSlotCountShift+1) {
		hm.minSlotCountShift++
	}
}

func (hm *HashMap) removeSlot() []protocol.HashItem {
	slotAddr := hm.locateSlotAddr(hm.slotCount - 1).Get(hm.fileStorage)
	items := hm.loadSlot(slotAddr)
	hm.eraseSlot(slotAddr)
	hm.slotCount--

	if hm.slotCount < 1<<hm.minSlotCountShift {
		hm.minSlotCountShift--
	}

	if hm.slotDirCount >= 2 && hm.slotCount == ((hm.slotDirCount-2)<<slotDirLengthShift)+1 {
		hm.removeSlotDir()
	}

	return items
}

func (hm *HashMap) addSlotDir() {
	if hm.slotDirCount == 1<<hm.maxSlotDirCountShift {
		hm.adjustSlotDirs(hm.maxSlotDirCountShift + 1)
	}

	slotDirAddr, _ := hm.fileStorage.AllocateSpace(8 << slotDirLengthShift)
	hm.locateSlotDirAddr(hm.slotDirCount).Set(hm.fileStorage, slotDirAddr)
	hm.slotDirCount++
}

func (hm *HashMap) removeSlotDir() {
	slotDirAddr := hm.locateSlotDirAddr(hm.slotDirCount - 1).Get(hm.fileStorage)
	hm.fileStorage.FreeSpace(slotDirAddr)
	hm.slotDirCount--

	if hm.maxSlotDirCountShift > minMaxSlotDirCountShift && hm.slotDirCount == (1<<(hm.maxSlotDirCountShift-2))+1 {
		hm.adjustSlotDirs(hm.maxSlotDirCountShift - 1)
	}
}

func (hm *HashMap) adjustSlotDirs(maxSlotDirCountShift int) {
	buffer1 := hm.fileStorage.AccessSpace(hm.slotDirsAddr)
	buffer2 := make([]byte, len(buffer1))
	copy(buffer2, buffer1)
	hm.fileStorage.FreeSpace(hm.slotDirsAddr)
	hm.slotDirsAddr, buffer1 = hm.fileStorage.AllocateSpace(8 << maxSlotDirCountShift)
	copy(buffer1, buffer2)
	hm.maxSlotDirCountShift = maxSlotDirCountShift
}

// Cursor represents a cursor at a position in a hash map.
type Cursor struct {
	items     []protocol.HashItem
	itemIndex int
	slotIndex int
}

const (
	minMaxSlotDirCountShift = 3
	slotDirLengthShift      = 12
	loadFactor              = 0.75
	maxShortKeySize         = 24
)

type addrRef struct {
	Addr  int64
	Index int
}

func (ar addrRef) Get(fileStorage *fsm.FileStorage) int64 {
	buffer := fileStorage.AccessSpace(ar.Addr)[ar.Index<<3:]
	return int64(binary.BigEndian.Uint64(buffer))
}

func (ar addrRef) Set(fileStorage *fsm.FileStorage, value int64) {
	buffer := fileStorage.AccessSpace(ar.Addr)[ar.Index<<3:]
	binary.BigEndian.PutUint64(buffer, uint64(value))
}

var errCorrupted = errors.New("hashmap: corrupted")

func sumKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

func matchItem(item *protocol.HashItem, key []byte, keySum uint64) bool {
	if len(item.Key) > maxShortKeySize && item.KeySum != keySum {
		return false
	}

	return bytes.Equal(item.Key, key)
}

func splitItems(items []protocol.HashItem, distinctKeySumBit uint64) ([]protocol.HashItem, []protocol.HashItem) {
	items2 := ([]protocol.HashItem)(nil)
	i := 0

	for j := range items {
		item := &items[j]
		var keySum uint64

		if len(item.Key) <= maxShortKeySize {
			keySum = sumKey(item.Key)
		} else {
			keySum = item.KeySum
		}

		if keySum&distinctKeySumBit != 0 {
			items2 = append(items2, *item)
			continue
		}

		items[i] = *item
		i++
	}

	items1 := items[:i]
	return items1, items2
}

func mergeItems(items1, items2 []protocol.HashItem) []protocol.HashItem {
	n1, n2 := len(items1), len(items2)
	items := make([]protocol.HashItem, n1+n2)
	var n int

	if n1 < n2 {
		n = n1
	} else {
		n = n2
	}

	for i := 0; i < n; i++ {
		item1 := &items1[i]
		item2 := &items2[i]
		x := (len(item1.Key) * len(item2.Key)) & 1
		items[(i<<1)|x] = *item1
		items[(i<<1)|(x^1)] = *item2
	}

	copy(items[n<<1:], items1[n:])
	copy(items[n<<1:], items2[n:])
	return items
}
