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
// then returns true, otherwise it returns false and the present
// value (optional) of the item.
func (hm *HashMap) AddItem(key []byte, value []byte, returnPresentValue bool) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := unpackSlot(hm.loadSlot(slotAddr))

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			if returnPresentValue {
				value = copyBytes(item.Value)
			} else {
				value = nil
			}

			return value, false
		}
	}

	hm.payloadSize += len(key) + len(value)

	if len(key) <= maxShortKeySize {
		// optimization for binary size
		keySum = 0
	}

	items = append(items, hashItem{
		KeySum: keySum,
		Key:    key,
		Value:  value,
	})

	slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, packSlot(items)))
	hm.postAddItem()
	return nil, true
}

// UpdateItem replaces the value of an item with the given key
// in the hash map to the given one.
// If an item matched exists in the hash map, it updates the item
// then returns true and the replaced value (optional) of the item,
// otherwise it returns false.
func (hm *HashMap) UpdateItem(key []byte, value []byte, returnReplacedValue bool) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := unpackSlot(hm.loadSlot(slotAddr))

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			hm.payloadSize += len(value) - len(item.Value)

			if returnReplacedValue {
				value, item.Value = copyBytes(item.Value), value
			} else {
				value, item.Value = nil, value
			}

			slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, packSlot(items)))
			return value, true
		}
	}

	return nil, false
}

// AddOrUpdateItem adds the given item to the hash map or replaces
// the value of an item with the given key to the given one.
// If no item matched exists in the hash map, it adds the item then
// returns true, otherwise it updates the item then returns false
// and the replaced value (optional) of the item.
func (hm *HashMap) AddOrUpdateItem(key []byte, value []byte, returnReplacedValue bool) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := unpackSlot(hm.loadSlot(slotAddr))

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			hm.payloadSize += len(value) - len(item.Value)

			if returnReplacedValue {
				value, item.Value = copyBytes(item.Value), value
			} else {
				value, item.Value = nil, value
			}

			slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, packSlot(items)))
			return value, false
		}
	}

	hm.payloadSize += len(key) + len(value)

	if len(key) <= maxShortKeySize {
		// optimization for binary size
		keySum = 0
	}

	items = append(items, hashItem{
		KeySum: keySum,
		Key:    key,
		Value:  value,
	})

	slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, packSlot(items)))
	hm.postAddItem()
	return nil, true
}

// DeleteItem deletes an item with the given key in the hash map.
// If an item matched exists in the hash map, it deletes the item
// then returns true and the removed value (optional) of the item,
// otherwise it returns false.
func (hm *HashMap) DeleteItem(key []byte, returnRemovedValue bool) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddrRef := hm.locateSlotAddr(hm.calculateSlotIndex(keySum))
	slotAddr := slotAddrRef.Get(hm.fileStorage)
	items := unpackSlot(hm.loadSlot(slotAddr))

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			hm.payloadSize -= len(item.Key) + len(item.Value)
			var value []byte

			if returnRemovedValue {
				value = copyBytes(item.Value)
			} else {
				value = nil
			}

			n := len(items)

			for j := i + 1; j < n; j++ {
				items[j-1] = items[j]
			}

			items = items[:n-1]
			slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, packSlot(items)))
			hm.postDeleteItem()
			return value, true
		}
	}

	return nil, false
}

// HasItem returns the value of an item with the given key in
// the hash map.
// If an item matched exists in the hash map, it returns true
// and the present value (optional) of the item, otherwise it
// returns false.
func (hm *HashMap) HasItem(key []byte, returnPresentValue bool) ([]byte, bool) {
	keySum := sumKey(key)
	slotAddr := hm.locateSlotAddr(hm.calculateSlotIndex(keySum)).Get(hm.fileStorage)
	items := unpackSlot(hm.loadSlot(slotAddr))

	for i := range items {
		item := &items[i]

		if matchItem(item, key, keySum) {
			var value []byte

			if returnPresentValue {
				value = copyBytes(item.Value)
			} else {
				value = nil
			}

			return value, true
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
		cursor.items = unpackSlot(hm.loadSlot(hm.locateSlotAddr(cursor.slotIndex).Get(hm.fileStorage)))
		cursor.slotIndex++

		if len(cursor.items) >= 1 {
			item := &cursor.items[0]
			cursor.itemIndex = 1
			return copyBytes(item.Key), copyBytes(item.Value), true
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
		ArrayAddr:    hm.locateSlotDirAddr(slotIndex >> slotDirLengthShift).Get(hm.fileStorage),
		ElementIndex: slotIndex & ((1 << slotDirLengthShift) - 1),
	}
}

func (hm *HashMap) locateSlotDirAddr(slotDirIndex int) addrRef {
	return addrRef{
		ArrayAddr:    hm.slotDirsAddr,
		ElementIndex: slotDirIndex,
	}
}

func (hm *HashMap) storeSlot(slot *protocol.HashSlot) int64 {
	if len(slot.ItemInfos) == 0 {
		return -1
	}

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

func (hm *HashMap) restoreSlot(slotAddr int64, slot *protocol.HashSlot) int64 {
	hm.eraseSlot(slotAddr)
	return hm.storeSlot(slot)
}

func (hm *HashMap) loadSlot(slotAddr int64) *protocol.HashSlot {
	if slotAddr < 0 {
		return &protocol.HashSlot{}
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

	return &slot
}

func (hm *HashMap) postAddItem() {
	hm.itemCount++
	hm.maybeExpand()
}

func (hm *HashMap) postDeleteItem() {
	hm.itemCount--
	hm.maybeShrink()
}

func (hm *HashMap) maybeExpand() {
	for float64(hm.itemCount)/float64(hm.slotCount) > loadFactor {
		slotIndex := hm.calculateParentSlotIndex(hm.slotCount)
		slotAddrRef := hm.locateSlotAddr(slotIndex)
		slotAddr := slotAddrRef.Get(hm.fileStorage)
		items1, items2 := splitItems(unpackSlot(hm.loadSlot(slotAddr)), uint64(1<<hm.minSlotCountShift))
		slot1, slot2 := packSlot(items1), packSlot(items2)
		slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, slot1))
		hm.addSlot(slot2)
	}
}

func (hm *HashMap) maybeShrink() {
	for hm.slotCount >= 2 && float64(hm.itemCount)/float64(hm.slotCount) <= loadFactor/2 {
		items1 := unpackSlot(hm.removeSlot())
		slotIndex := hm.calculateParentSlotIndex(hm.slotCount)
		slotAddrRef := hm.locateSlotAddr(slotIndex)
		slotAddr := slotAddrRef.Get(hm.fileStorage)
		items2 := unpackSlot(hm.loadSlot(slotAddr))
		slotAddrRef.Set(hm.fileStorage, hm.restoreSlot(slotAddr, packSlot(mergeItems(items1, items2))))
	}
}

func (hm *HashMap) addSlot(slot *protocol.HashSlot) {
	if hm.slotCount == hm.slotDirCount<<slotDirLengthShift {
		hm.addSlotDir()
	}

	hm.locateSlotAddr(hm.slotCount).Set(hm.fileStorage, hm.storeSlot(slot))
	hm.slotCount++

	if hm.slotCount == 1<<(hm.minSlotCountShift+1) {
		hm.minSlotCountShift++
	}
}

func (hm *HashMap) removeSlot() *protocol.HashSlot {
	slotAddr := hm.locateSlotAddr(hm.slotCount - 1).Get(hm.fileStorage)
	slot := hm.loadSlot(slotAddr)
	slot.Bin = copyBytes(slot.Bin)
	hm.eraseSlot(slotAddr)
	hm.slotCount--

	if hm.slotCount < 1<<hm.minSlotCountShift {
		hm.minSlotCountShift--
	}

	if hm.slotDirCount >= 2 && hm.slotCount == ((hm.slotDirCount-2)<<slotDirLengthShift)+1 {
		hm.removeSlotDir()
	}

	return slot
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
	items     []hashItem
	itemIndex int
	slotIndex int
}

const (
	minMaxSlotDirCountShift = 3
	slotDirLengthShift      = 12
	loadFactor              = 1.61803398874989484820458683436563811772030917980576286213544862270526046281890244970720720418939113748475
	maxShortKeySize         = 24
)

type addrRef struct {
	ArrayAddr    int64
	ElementIndex int
}

func (ar addrRef) Get(fileStorage *fsm.FileStorage) int64 {
	buffer := fileStorage.AccessSpace(ar.ArrayAddr)[ar.ElementIndex<<3:]
	return int64(binary.BigEndian.Uint64(buffer))
}

func (ar addrRef) Set(fileStorage *fsm.FileStorage, value int64) {
	buffer := fileStorage.AccessSpace(ar.ArrayAddr)[ar.ElementIndex<<3:]
	binary.BigEndian.PutUint64(buffer, uint64(value))
}

type hashItem struct {
	KeySum uint64
	Key    []byte
	Value  []byte
}

var errCorrupted = errors.New("hashmap: corrupted")

func sumKey(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	return h.Sum64()
}

func matchItem(item *hashItem, key []byte, keySum uint64) bool {
	if len(item.Key) > maxShortKeySize && item.KeySum != keySum {
		return false
	}

	return bytes.Equal(item.Key, key)
}

func packSlot(items []hashItem) *protocol.HashSlot {
	n := len(items)

	if n == 0 {
		return &protocol.HashSlot{}
	}

	i := 0

	for j := range items {
		item := &items[j]
		i += len(item.Key) + len(item.Value)
	}

	slot := protocol.HashSlot{
		ItemInfos: make([]protocol.HashItemInfo, n),
		Bin:       make([]byte, i),
	}

	i = 0

	for j := range items {
		item := &items[j]
		itemInfo := &slot.ItemInfos[j]
		itemInfo.KeySum = item.KeySum
		copy(slot.Bin[i:], item.Key)
		itemInfo.KeySize = int64(len(item.Key))
		i += len(item.Key)
		copy(slot.Bin[i:], item.Value)
		itemInfo.ValueSize = int64(len(item.Value))
		i += len(item.Value)
	}

	// optimization for binary size
	slot.ItemInfos[n-1].ValueSize = 0
	return &slot
}

func unpackSlot(slot *protocol.HashSlot) []hashItem {
	n := len(slot.ItemInfos)

	if n == 0 {
		return nil
	}

	items := make([]hashItem, n)
	i := 0

	for j := range slot.ItemInfos {
		itemInfo := &slot.ItemInfos[j]
		item := &items[j]
		item.KeySum = itemInfo.KeySum
		item.Key = slot.Bin[i : i+int(itemInfo.KeySize)]
		i += int(itemInfo.KeySize)
		item.Value = slot.Bin[i : i+int(itemInfo.ValueSize)]
		i += int(itemInfo.ValueSize)
	}

	// cost of optimization for binary size
	items[n-1].Value = slot.Bin[i:]
	return items
}

func splitItems(items []hashItem, distinctKeySumBit uint64) ([]hashItem, []hashItem) {
	items2 := ([]hashItem)(nil)
	i := 0

	for j := range items {
		item := &items[j]
		var keySum uint64

		if len(item.Key) <= maxShortKeySize {
			// cost of optimization for binary size
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

func mergeItems(items1, items2 []hashItem) []hashItem {
	n1, n2 := len(items1), len(items2)
	items := make([]hashItem, n1+n2)
	var n int

	if n1 < n2 {
		n = n1
	} else {
		n = n2
	}

	x := uint64(1)

	for i := 0; i < n; i++ {
		item1 := &items1[i]
		item2 := &items2[i]
		x *= uint64(len(item1.Key)) + uint64(len(item2.Key))
		j := int(x & 1)
		items[(i<<1)|j] = *item1
		items[(i<<1)|(j^1)] = *item2
	}

	copy(items[n<<1:], items1[n:])
	copy(items[n<<1:], items2[n:])
	return items
}

func copyBytes(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}

	buffer := make([]byte, len(data))
	copy(buffer, data)
	return buffer
}
