package hashmap_test

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/roy2220/fsm"
	"github.com/roy2220/plainkv/internal/hashmap"
	"github.com/stretchr/testify/assert"
)

var KVs [][]byte

func TestHashMapStoreAndLoad(t *testing.T) {
	n := 100000
	hm, cleanup := MakeHashMap(t, &n)
	hm.Load(hm.Store())
	defer cleanup()

	for i := 0; i < n; i++ {
		k := KVs[i]
		v := KVs[len(KVs)/2+i]
		v2, ok := hm.HasItem(k)

		if assert.True(t, ok) {
			assert.Equal(t, v, v2)
		}
	}
}

func TestHashMapUpdateItem(t *testing.T) {
	n := 100000
	hm, cleanup := MakeHashMap(t, &n)
	defer cleanup()

	for i := 0; i < n; i++ {
		k := KVs[i]
		v := KVs[len(KVs)/2+i]
		v2 := strconv.AppendInt(make([]byte, 0, 6), int64(i), 10)
		v3, ok := hm.UpdateItem(k, v2)

		if assert.True(t, ok) {
			assert.Equal(t, v, v3)
		}
	}

	for i := 0; i < n; i++ {
		k := KVs[i]
		v, ok := hm.HasItem(k)

		if assert.True(t, ok) {
			j, err := strconv.ParseInt(string(v), 10, 32)

			if assert.NoError(t, err) {
				assert.Equal(t, int(j), i)
			}
		}
	}
}

func TestHashMapAddOrUpdateItem(t *testing.T) {
	n := 100000 / 2
	hm, cleanup := MakeHashMap(t, &n)
	defer cleanup()

	for i := 0; i < n; i++ {
		k := KVs[i]
		v := KVs[len(KVs)/2+i]
		v2 := strconv.AppendInt(make([]byte, 0, 6), int64(i), 10)
		v3, ok := hm.AddOrUpdateItem(k, v2)

		if assert.False(t, ok) {
			assert.Equal(t, v, v3)
		}
	}

	for i := n; i < 2*n; i++ {
		k := KVs[i]
		v := strconv.AppendInt(make([]byte, 0, 6), int64(i), 10)
		_, ok := hm.AddOrUpdateItem(k, v)
		assert.True(t, ok)
	}

	for i := 0; i < 2*n; i++ {
		k := KVs[i]
		v, ok := hm.HasItem(k)

		if assert.True(t, ok) {
			j, err := strconv.ParseInt(string(v), 10, 32)

			if assert.NoError(t, err) {
				assert.Equal(t, int(j), i)
			}
		}
	}
}

func TestHashMapDeleteItem(t *testing.T) {
	n := 100000
	hm, fs, cleanup := DoMakeHashMap(t, &n)
	defer cleanup()
	tab := make([]int32, n)

	for i := range tab {
		tab[i] = int32(i)
	}

	rand.Shuffle(len(tab), func(i, j int) {
		tab[i], tab[j] = tab[j], tab[i]
	})

	var i int

	for i = 0; i < len(tab)/2; i++ {
		j := int(tab[i])
		k := KVs[j]
		v := KVs[len(KVs)/2+j]
		v2, ok := hm.DeleteItem(k)

		if assert.True(t, ok) {
			assert.Equal(t, v, v2)
		}
	}

	for ; i < len(tab); i++ {
		j := int(tab[i])
		k := KVs[j]
		v := KVs[len(KVs)/2+j]
		v2, ok := hm.HasItem(k)

		if assert.True(t, ok) {
			assert.Equal(t, v, v2)
		}
	}

	hm.Load(hm.Store())

	for i--; i >= len(tab)/2; i-- {
		j := int(tab[i])
		k := KVs[j]
		v := KVs[len(KVs)/2+j]
		v2, ok := hm.DeleteItem(k)

		if assert.True(t, ok) {
			assert.Equal(t, v, v2)
		}
	}

	for i := 0; i < n; i++ {
		k := KVs[i]
		_, ok := hm.HasItem(k)
		assert.False(t, ok)
	}

	t.Logf("fsm_stats=%#v, num_slot_dirs=%#v, num_slots=%#v, num_items=%#v, payload_size=%#v",
		fs.Stats(), hm.NumberOfSlotDirs(), hm.NumberOfSlots(), hm.NumberOfItems(), hm.PayloadSize())
	assert.Equal(t, hm.PayloadSize(), 0)
	assert.Equal(t, hm.NumberOfItems(), 0)
	assert.Equal(t, hm.NumberOfSlots(), 1)
	assert.Equal(t, hm.MinNumberOfSlots(), 1)
	assert.Equal(t, hm.NumberOfSlotDirs(), 1)
	assert.Equal(t, hm.MaxNumberOfSlotDirs(), 8)
	hm.Destroy()
	st := fs.Stats()

	if !assert.Equal(t, 0, st.AllocatedSpaceSize) {
		t.Fatal()
	}

	hm.Create()
}

func TestHashMapFetchItem(t *testing.T) {
	n := 100000
	hm, cleanup := MakeHashMap(t, &n)
	defer cleanup()
	m := make(map[string]string, n)

	for i := 0; i < n; i++ {
		k := KVs[i]
		v := KVs[len(KVs)/2+i]
		m[string(k)] = string(v)
	}

	c := hashmap.Cursor{}

	for k, v, ok := hm.FetchItem(&c); ok; k, v, ok = hm.FetchItem(&c) {
		sk := string(k)
		sv, ok := m[sk]

		if assert.True(t, ok) {
			delete(m, sk)
			assert.Equal(t, sv, string(v))
		}
	}

	assert.Equal(t, 0, len(m))
}

func MakeHashMap(t *testing.T, numberOfHashItems *int) (*hashmap.HashMap, func()) {
	hm, _, cleanup := DoMakeHashMap(t, numberOfHashItems)
	return hm, cleanup
}

func DoMakeHashMap(t *testing.T, numberOfHashItems *int) (*hashmap.HashMap, *fsm.FileStorage, func()) {
	const fn = "../../test/hashmap.tmp"
	fs := new(fsm.FileStorage).Init()
	err := fs.Open(fn, true)

	if !assert.NoError(t, err) {
		t.FailNow()
	}

	hm := new(hashmap.HashMap).Init(fs)
	hm.Create()
	m := len(KVs) / 2

	if *numberOfHashItems < 0 || *numberOfHashItems > m {
		*numberOfHashItems = m
	}

	for i := 0; i < *numberOfHashItems; i++ {
		_, ok := hm.AddItem(KVs[i], KVs[m+i])

		if !assert.True(t, ok) {
			t.FailNow()
		}
	}

	if !assert.Equal(t, hm.NumberOfItems(), *numberOfHashItems) {
		t.FailNow()
	}

	t.Logf("fsm_stats=%#v, num_slot_dirs=%#v, num_slots=%#v, num_items=%#v, payload_size=%#v",
		fs.Stats(), hm.NumberOfSlotDirs(), hm.NumberOfSlots(), hm.NumberOfItems(), hm.PayloadSize())

	return hm, fs, func() {
		hm.Destroy()
		fs.Close()
		os.Remove(fn)
	}
}

func TestMain(m *testing.M) {
	data, err := ioutil.ReadFile("../../test/data/10-million-password-list-top-1000000.txt")

	if err != nil {
		panic(err)
	}

	a := bytes.Split(data[:len(data)-1], []byte{'\n'})
	KVs = make([][]byte, 2*len(a))
	copy(KVs, a)

	rand.Shuffle(len(a), func(i, j int) {
		a[i], a[j] = a[j], a[i]
	})

	copy(KVs[len(a):], a)
	os.Exit(m.Run())
}
