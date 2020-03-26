package bptree_test

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/roy2220/fsm"
	"github.com/roy2220/plainkv/bptree"
	"github.com/stretchr/testify/assert"
)

func TestBPTreeAddOrUpdateAndHasRecord(t *testing.T) {
	bpt, _, cleanup := MakeBPTree(t)
	defer cleanup()

	for i, k := range Keywords {
		v := []byte(strconv.Itoa(i))
		k2, ok := bpt.AddOrUpdateRecord(k, v, true)

		if assert.False(t, ok) {
			assert.Equal(t, k, k2)
		}
	}

	_, ok := bpt.AddOrUpdateRecord([]byte("K4cM,b/PaY;4Hb[A]"), nil, false)
	assert.True(t, ok)
	_, ok = bpt.AddRecord([]byte("K4cM,b/PaY;4Hb[A]"), nil, true)
	assert.False(t, ok)

	for i, k := range Keywords {
		v, ok := bpt.HasRecord(k, true)

		if assert.True(t, ok) {
			v2 := []byte(strconv.Itoa(i))
			assert.Equal(t, v2, v)
		}
	}

	_, ok = bpt.HasRecord([]byte("K8=JT6!xcH@m;9tf"), false)
	assert.False(t, ok)
}

func TestBPTreeUpdateAndDeleteRecord(t *testing.T) {
	bpt, fs, cleanup := MakeBPTree(t)
	defer cleanup()

	for i, k := range Keywords {
		v := []byte(strconv.Itoa(i))
		k2, ok := bpt.UpdateRecord(k, v, true)

		if assert.True(t, ok) {
			if !assert.Equal(t, k, k2) {
				t.FailNow()
			}
		} else {
			t.FailNow()
		}
	}

	_, ok := bpt.UpdateRecord([]byte("K4cM,b/PaY;4Hb[A]"), nil, false)
	assert.False(t, ok)

	for i, k := range Keywords {
		v, ok := bpt.DeleteRecord(k, true)

		if assert.True(t, ok) {
			v2 := []byte(strconv.Itoa(i))
			if !assert.Equal(t, v2, v) {
				t.FailNow()
			}
		} else {
			t.FailNow()
		}
	}

	_, ok = bpt.DeleteRecord([]byte("K8=JT6!xcH@m;9tf"), false)
	t.Logf("height=%d, num_leafs=%d num_non_leafs=%d payload_size=%d fs_stats=%#v",
		bpt.Height(), bpt.NumberOfLeafs(), bpt.NumberOfNonLeafs(), bpt.PayloadSize(), fs.Stats())
	assert.Equal(t, 0, bpt.PayloadSize())
	assert.False(t, ok)

	bpt.Destroy()
	assert.Equal(t, 0, fs.Stats().AllocatedSpaceSize)
	bpt.Create()
}

func TestBPTreeSearchForwardAndBackward(t *testing.T) {
	bpt, _, cleanup := MakeBPTree(t)
	defer cleanup()
	i := 0

	for it := bpt.SearchForward(bptree.MinKey, bptree.MaxKey); !it.IsAtEnd(); it.Advance() {
		k := Keywords[SortedKeywordIndexes[i]]
		i++
		k2, _ := it.ReadKey()
		if !assert.Equal(t, k2, k) {
			t.FailNow()
		}
	}

	assert.Equal(t, len(Keywords), i)
	i--

	for it := bpt.SearchBackward(bptree.MinKey, bptree.MaxKey); !it.IsAtEnd(); it.Advance() {
		k := Keywords[SortedKeywordIndexes[i]]
		i--
		k2, _ := it.ReadKey()
		if !assert.Equal(t, k2, k) {
			t.FailNow()
		}
	}

	assert.Equal(t, -1, i)
	minKey := Keywords[SortedKeywordIndexes[0]]
	maxKey := Keywords[SortedKeywordIndexes[len(Keywords)-1]]

	{
		it := bpt.SearchForward(bptree.MaxKey, bptree.MinKey)
		assert.True(t, it.IsAtEnd())
		it = bpt.SearchForward(maxKey, bptree.MinKey)
		assert.True(t, it.IsAtEnd())
		it = bpt.SearchForward(bptree.MaxKey, minKey)
		assert.True(t, it.IsAtEnd())
		it = bpt.SearchForward(maxKey, minKey)
		assert.True(t, it.IsAtEnd())
	}

	{
		it := bpt.SearchForward(bptree.MinKey, bptree.MinKey)
		minKey1, _ := it.ReadKey()
		assert.Equal(t, minKey, minKey1)
		it.Advance()
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchForward(minKey, bptree.MinKey)
		minKey2, _ := it.ReadKey()
		assert.Equal(t, minKey, minKey2)
		it.Advance()
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchForward(bptree.MinKey, minKey)
		minKey3, _ := it.ReadKey()
		assert.Equal(t, minKey, minKey3)
		it.Advance()
		assert.True(t, it.IsAtEnd())
	}

	{
		it := bpt.SearchBackward(bptree.MaxKey, bptree.MaxKey)
		maxKey1, _ := it.ReadKey()
		assert.Equal(t, maxKey, maxKey1)
		it.Advance()
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchBackward(maxKey, bptree.MaxKey)
		maxKey2, _ := it.ReadKey()
		assert.Equal(t, maxKey, maxKey2)
		it.Advance()
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchBackward(bptree.MaxKey, maxKey)
		maxKey3, _ := it.ReadKey()
		assert.Equal(t, maxKey, maxKey3)
		it.Advance()
		assert.True(t, it.IsAtEnd())
	}
}

func TestBPTreeStoreAndLoad(t *testing.T) {
	bpt, _, cleanup := MakeBPTree(t)
	defer cleanup()
	bpt.Load(bpt.Store())
	i := 0

	for it := bpt.SearchForward(bptree.MinKey, bptree.MaxKey); !it.IsAtEnd(); it.Advance() {
		k := Keywords[SortedKeywordIndexes[i]]
		i++
		k2, _ := it.ReadKey()
		if !assert.Equal(t, k2, k) {
			t.FailNow()
		}
	}

	assert.Equal(t, len(Keywords), i)
}

func _TestBPTreeFprint(t *testing.T) {
	bpt, _, cleanup := MakeBPTree(t)
	defer cleanup()
	bpt.Fprint(os.Stdout)
}

var Keywords [][]byte
var SortedKeywordIndexes []int
var SortedKeywordRIndexes []int

func MakeBPTree(t *testing.T) (*bptree.BPTree, *fsm.FileStorage, func()) {
	const fn = "../test/bptree.tmp"
	fs := new(fsm.FileStorage).Init()
	err := fs.Open(fn, true)

	if !assert.NoError(t, err) {
		t.FailNow()
	}

	bpt := new(bptree.BPTree).Init(fs)
	bpt.Create()
	deletedKeywordIndexes := make(map[int]struct{}, len(Keywords)/2)

	for i, k := range Keywords {
		k2, ok := bpt.AddRecord(k, k, true)

		if !assert.True(t, ok, string(k), string(k2)) {
			t.FailNow()
		}

		j := rand.Intn(i*2 + 1)

		if j <= i {
			if _, ok := deletedKeywordIndexes[j]; !ok {
				k := Keywords[j]
				k2, ok2 := bpt.DeleteRecord(k, true)

				if !assert.True(t, ok2, "%v %s", j, k) {
					t.FailNow()
				}

				assert.Equal(t, k, k2)
				deletedKeywordIndexes[j] = struct{}{}
			}
		}
	}

	for j := range deletedKeywordIndexes {
		k := Keywords[j]
		_, ok := bpt.AddRecord(k, k, false)

		if !assert.True(t, ok, "%v %s", j, k) {
			t.FailNow()
		}
	}

	t.Logf("height=%d, num_leafs=%d num_non_leafs=%d payload_size=%d fs_stats=%#v",
		bpt.Height(), bpt.NumberOfLeafs(), bpt.NumberOfNonLeafs(), bpt.PayloadSize(), fs.Stats())

	return bpt, fs, func() {
		bpt.Destroy()
		fs.Close()
		os.Remove(fn)
	}
}

func TestMain(m *testing.M) {
	data, err := ioutil.ReadFile("../test/data/10-million-password-list-top-1000000.txt")

	if err != nil {
		panic(err)
	}

	Keywords = bytes.Split(data, []byte("\n"))
	Keywords = Keywords[:len(Keywords)-1]
	Keywords = Keywords[:1000000]
	SortedKeywordIndexes = make([]int, len(Keywords))

	for i := range Keywords {
		SortedKeywordIndexes[i] = i
	}

	sort.Slice(SortedKeywordIndexes, func(i, j int) bool {
		return bytes.Compare(Keywords[SortedKeywordIndexes[i]], Keywords[SortedKeywordIndexes[j]]) < 0
	})

	SortedKeywordRIndexes = make([]int, len(SortedKeywordIndexes))

	for r, i := range SortedKeywordIndexes {
		SortedKeywordRIndexes[i] = r
	}

	os.Exit(m.Run())
}
