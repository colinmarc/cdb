package cdb

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

const maxUint32 = int64(^uint32(0))

var ErrTooMuchData = errors.New("CDB files are limited to 4GB of data")

// Writer lets you write out a CDB database record by record. The database
// is not complete until Close or Freeze is called.
type Writer struct {
	writer       io.WriteSeeker
	entries      [256][]entry
	finalizeOnce sync.Once
}

type entry struct {
	hash   uint32
	offset uint32
}

// Create opens a CDB database at the given path. If the file exists, it will
// be overwritten.
func Create(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	return NewWriter(f)
}

// NewWriter opens a CDB database for the given io.WriteSeeker.
func NewWriter(writer io.WriteSeeker) (*Writer, error) {
	// Leave 256 * 8 bytes for the index at the head of the file.
	_, err := writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(make([]byte, indexSize))
	if err != nil {
		return nil, err
	}

	return &Writer{writer: writer}, nil
}

// Put adds a key/value pair to the database.
func (cdb *Writer) Put(key, value []byte) error {
	off, err := cdb.writer.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if off > maxUint32 {
		return ErrTooMuchData
	}

	digest := newCDBHash()
	digest.Write(key)
	hash := digest.Sum32()
	table := hash & 0xff
	cdb.entries[table] = append(cdb.entries[table], entry{hash: hash, offset: uint32(off)})

	err = writeTuple(cdb.writer, uint32(len(key)), uint32(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.writer.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.writer.Write(value)
	if err != nil {
		return err
	}

	return nil
}

// Close finalizes the database, then closes it to further writes.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Close() error {
	var err error
	cdb.finalizeOnce.Do(func() {
		_, err = cdb.finalize()
	})

	if err != nil {
		return err
	}

	if closer, ok := cdb.writer.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

// Freeze finalizes the database, then opens it for reads. If the stream cannot
// be converted to a io.ReaderAt, Freeze will return os.ErrInvalid.
//
// Close or Freeze must be called to finalize the database, or the resulting
// file will be invalid.
func (cdb *Writer) Freeze() (*CDB, error) {
	var err error
	var index index
	cdb.finalizeOnce.Do(func() {
		index, err = cdb.finalize()
	})

	if err != nil {
		return nil, err
	}

	if readerAt, ok := cdb.writer.(io.ReaderAt); ok {
		return &CDB{reader: readerAt, index: index}, nil
	} else {
		return nil, os.ErrInvalid
	}
}

func (cdb *Writer) finalize() (index, error) {
	var index index

	// Write the hashtables out, one by one, at the end of the file.
	for i := 0; i < 256; i++ {
		off, err := cdb.writer.Seek(0, os.SEEK_CUR)
		if err != nil {
			return index, err
		}

		if off > maxUint32 {
			return index, ErrTooMuchData
		}

		tableEntries := cdb.entries[i]
		index[i] = table{offset: uint32(off), length: uint32(len(tableEntries))}
		for _, entry := range tableEntries {
			err := writeTuple(cdb.writer, entry.hash, entry.offset)
			if err != nil {
				return index, err
			}
		}
	}

	// Then, seek to the beginning of the file and write out the index.
	_, err := cdb.writer.Seek(0, os.SEEK_SET)
	if err != nil {
		return index, err
	}

	buf := make([]byte, indexSize)
	for i, table := range index {
		off := i * 8
		binary.LittleEndian.PutUint32(buf[off:off+4], table.offset)
		binary.LittleEndian.PutUint32(buf[off+4:off+8], table.length)
	}

	_, err = cdb.writer.Write(buf)
	if err != nil {
		return index, err
	}

	return index, nil
}
