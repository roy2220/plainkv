package protocol

// BytesView represents a read-only reference to bytes.
type BytesView []byte

// Size returns the number of the bytes.
func (bv BytesView) Size() int { return len(bv) }

// MarshalTo copys the bytes to the given buffer.
func (bv BytesView) MarshalTo(buffer []byte) (int, error) { return copy(buffer, bv), nil }

// Unmarshal refers the data as the bytes.
func (bv *BytesView) Unmarshal(data []byte) error { *bv = data; return nil }
