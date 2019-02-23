package cdb

const start uint32 = 5381

func cdbHash(data []byte) uint32 {
	v := start
	for _, b := range data {
		v = ((v << 5) + v) ^ uint32(b)
	}

	return v
}
