package cdb

import (
	"encoding/binary"
)

const start = 5381

type cdbHash struct {
	uint32
}

func newCDBHash() *cdbHash {
	return &cdbHash{start}
}

func (h *cdbHash) Write(data []byte) (int, error) {
	h.uint32 = CDBHashSum32Update (data, h.uint32)
	return len(data), nil
}

func CDBHashSum32(data []byte) (uint32) {
	return CDBHashSum32Update(data, start)
}

func CDBHashSum32Update(data []byte, v uint32) (uint32) {
	for _, b := range data {
		v = ((v << 5) + v) ^ uint32(b)
	}
	return v
}

func (h *cdbHash) Sum(b []byte) []byte {
	digest := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, h.Sum32())

	return append(b, digest...)
}

func (h *cdbHash) Sum32() uint32 {
	return h.uint32
}

func (h *cdbHash) Reset() {
	h.uint32 = start
}

func (h *cdbHash) Size() int {
	return 4
}

func (h *cdbHash) BlockSize() int {
	return 4
}
