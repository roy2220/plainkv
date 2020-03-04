package plainkv_test

import (
	"fmt"

	"github.com/roy2220/plainkv"
)

func Example() {
	func() {
		dict, err := plainkv.OpenDict("./test/dict.tmp", true)

		if err != nil {
			panic(err)
		}

		defer dict.Close()

		dict.Set([]byte("foo"), []byte("bar"))

		v, ok := dict.SetIfNotExists([]byte("hello"), []byte("word"))
		fmt.Printf("%v %q\n", ok, string(v))

		v, ok = dict.SetIfExists([]byte("hello"), []byte("world"))
		fmt.Printf("%v %q\n", ok, string(v))
	}()

	func() {
		dict, err := plainkv.OpenDict("./test/dict.tmp", false)

		if err != nil {
			panic(err)
		}

		defer dict.Close()

		v, ok := dict.Get([]byte("foo"))
		fmt.Printf("%v %q\n", ok, string(v))

		v, ok = dict.Clear([]byte("hello"))
		fmt.Printf("%v %q\n", ok, string(v))

		v, ok = dict.Get([]byte("hello"))
		fmt.Printf("%v %q\n", ok, string(v))
	}()
	// Output:
	// true ""
	// true "word"
	// true "bar"
	// true "world"
	// false ""
}
