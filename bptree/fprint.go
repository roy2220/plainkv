package bptree

import (
	"fmt"
	"io"
)

// Fprint dumps the B+ tree as plain text for debugging purposes.
func (bpt *BPTree) Fprint(writer io.Writer) error {
	return bpt.doFprint(writer, bpt.rootAddr, 1, "", "\n")
}

func (bpt *BPTree) doFprint(writer io.Writer, nodeAddr int64, nodeDepth int, prefix, newLine string) error {
	if nodeDepth == bpt.height {
		leafController := bpt.getLeafController(nodeAddr)
		n := leafController.NumberOfRecords()

		for i := 0; i < n; i++ {
			key := keyFactory{bpt.fileStorage}.ReadKey(leafController.GetKey(i))
			value := valueFactory{bpt.fileStorage}.ReadValue(leafController.GetValue(i))
			var err error

			switch i {
			case 0:
				if n == 1 {
					_, err = fmt.Fprintf(writer, "%s──● %q=%q", prefix, key, value)
				} else {
					_, err = fmt.Fprintf(writer, "%s┬─● %q=%q", prefix, key, value)
				}
			case n - 1:
				_, err = fmt.Fprintf(writer, "%s└─● %q=%q", newLine, key, value)
			default:
				_, err = fmt.Fprintf(writer, "%s├─● %q=%q", newLine, key, value)
			}

			if err != nil {
				return err
			}
		}
	} else {
		nonLeafController := bpt.getNonLeafController(nodeAddr)

		if err := bpt.doFprint(writer, nonLeafController.GetChildAddr(0), nodeDepth+1, prefix+"┬─", newLine+"│ "); err != nil {
			return err
		}

		n := nonLeafController.NumberOfChildren()

		for i := 1; i < n; i++ {
			key := keyFactory{bpt.fileStorage}.ReadKey(nonLeafController.GetKey(i))

			if _, err := fmt.Fprintf(writer, "%s├─● %q", newLine, key); err != nil {
				return err
			}

			var prefix2, newLine2 string

			if i == n-1 {
				prefix2, newLine2 = newLine+"└─", newLine+"  "
			} else {
				prefix2, newLine2 = newLine+"├─", newLine+"│ "
			}

			if err := bpt.doFprint(writer, nonLeafController.GetChildAddr(i), nodeDepth+1, prefix2, newLine2); err != nil {
				return err
			}
		}
	}

	return nil
}
