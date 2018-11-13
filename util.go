package cdb

import (
	"encoding/binary"
	"io"
)

func (cdb *CDB) readAt(offset uint32, size uint32) ([]byte, error) {
	var buf []byte
	if cdb.readerBytes == nil {
		buf = make([]byte, size)
		_, err := cdb.reader.ReadAt(buf, int64 (offset))
		if err != nil {
			return nil, err
		}
	} else {
		buf = cdb.readerBytes[offset : offset + size]
	}
	return buf, nil
}

func (cdb *CDB) readTuple(offset uint32) (uint32, uint32, error) {
	tuple, err := cdb.readAt(offset, 8)
	if err != nil {
		return 0, 0, err
	}

	first := binary.LittleEndian.Uint32(tuple[:4])
	second := binary.LittleEndian.Uint32(tuple[4:])
	return first, second, nil
}

func writeTuple(w io.Writer, first, second uint32) error {
	tuple := make([]byte, 8)
	binary.LittleEndian.PutUint32(tuple[:4], first)
	binary.LittleEndian.PutUint32(tuple[4:], second)

	_, err := w.Write(tuple)
	return err
}
