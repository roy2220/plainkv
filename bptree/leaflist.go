package bptree

import "github.com/roy2220/fsm"

type leafList struct {
	tailAddr int64
	headAddr int64
}

func (ll *leafList) Init(leafAccessor []byte, leafAddr int64) *leafList {
	leafHeader1 := leafHeader(leafAccessor)
	leafHeader1.SetPrevAddr(leafAddr)
	leafHeader1.SetNextAddr(leafAddr)
	ll.tailAddr = leafAddr
	ll.headAddr = leafAddr
	return ll
}

func (ll *leafList) InsertLeafAfter(fileStorage *fsm.FileStorage, leafAddr int64, leafPrevAddr int64) {
	leafFactory := leafFactory{fileStorage}
	leafHeader1 := leafHeader(leafFactory.GetLeafController(leafAddr))
	leafPrevHeader := leafHeader(leafFactory.GetLeafController(leafPrevAddr))
	leafNextAddr := leafPrevHeader.NextAddr()
	leafNextHeader := leafHeader(leafFactory.GetLeafController(leafNextAddr))
	leafHeader1.SetPrevAddr(leafPrevAddr)
	leafPrevHeader.SetNextAddr(leafAddr)
	leafHeader1.SetNextAddr(leafNextAddr)
	leafNextHeader.SetPrevAddr(leafAddr)

	if leafPrevAddr == ll.tailAddr {
		ll.tailAddr = leafAddr
	}
}

func (ll *leafList) RemoveLeaf(fileStorage *fsm.FileStorage, leafAddr int64) {
	leafFactory := leafFactory{fileStorage}
	leafHeader1 := leafHeader(leafFactory.GetLeafController(leafAddr))
	leafPrevAddr := leafHeader1.PrevAddr()
	leafPrevHeader := leafHeader(leafFactory.GetLeafController(leafPrevAddr))
	leafNextAddr := leafHeader1.NextAddr()
	leafNextHeader := leafHeader(leafFactory.GetLeafController(leafNextAddr))
	leafPrevHeader.SetNextAddr(leafNextAddr)
	leafNextHeader.SetPrevAddr(leafPrevAddr)

	if leafAddr == ll.headAddr {
		ll.headAddr = leafNextAddr
	} else if leafAddr == ll.tailAddr {
		ll.tailAddr = leafPrevAddr
	}
}

func (ll *leafList) TailAddr() int64 {
	return ll.tailAddr
}

func (ll *leafList) HeadAddr() int64 {
	return ll.headAddr
}
