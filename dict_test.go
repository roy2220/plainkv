package plainkv_test

import (
	"fmt"

	"github.com/roy2220/plainkv"
)

func ExampleDict() {
	func() {
		d, err := plainkv.OpenDict("./test/dict.tmp", true)
		if err != nil {
			panic(err)
		}
		defer d.Close()

		d.Set([]byte("foo"), []byte("bar"), false /* don't return the replaced value */)

		_, ok := d.SetIfNotExists([]byte("hello"), []byte("w0rd"), false /* don't return the present value */)
		fmt.Printf("%v\n", ok)

		v, ok := d.SetIfExists([]byte("hello"), []byte("world"), true /* return the replaced value */)
		fmt.Printf("%v %q\n", ok, v)
	}()

	func() {
		d, err := plainkv.OpenDict("./test/dict.tmp", false)
		if err != nil {
			panic(err)
		}
		defer d.Close()

		dc := plainkv.DictCursor{}
		for {
			k, v, ok := d.Scan(&dc)
			if !ok {
				break
			}
			fmt.Printf("%q %q\n", k, v)
		}

		v, ok := d.Test([]byte("foo"), true /* return the present value */)
		fmt.Printf("%v %q\n", ok, v)

		v, ok = d.Clear([]byte("hello"), true /* return the removed value */)
		fmt.Printf("%v %q\n", ok, v)
	}()
	// Output:
	// true
	// true "w0rd"
	// "foo" "bar"
	// "hello" "world"
	// true "bar"
	// true "world"
}
