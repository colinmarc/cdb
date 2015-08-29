package cdb

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"bufio"
)

const maxUint32 = int64(^uint32(0))

var ErrTooMuchData = errors.New("CDB files are limited to 4GB of data")

// Writer lets you write out a CDB database record by record. The database
// is not complete until Close or Freeze is called.
type Writer struct {
	writer       io.WriteSeeker
	entries      [256][]entry
	finalizeOnce sync.Once

	bufferedWriter *bufio.Writer
	bufferedOffset int64
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

	return &Writer{
		writer: writer,
		bufferedWriter: bufio.NewWriter(writer),
		bufferedOffset: indexSize,
	}, nil
}

// Put adds a key/value pair to the database. If the amount of data written
// would exceed the limit, Put returns ErrTooMuchData.
func (cdb *Writer) Put(key, value []byte) error {
	// Record the entry in the hash table, to be written out at the end.
  digest := newCDBHash()
	digest.Write(key)
	hash := digest.Sum32()
	table := hash & 0xff
	entry := entry{hash: hash, offset: uint32(cdb.bufferedOffset)}
	cdb.entries[table] = append(cdb.entries[table], entry)

	// Write the key length, then value length, then key, then value.
	err := writeTuple(cdb.bufferedWriter, uint32(len(key)), uint32(len(value)))
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(key)
	if err != nil {
		return err
	}

	_, err = cdb.bufferedWriter.Write(value)
	if err != nil {
		return err
	}

	cdb.bufferedOffset += int64(8 + len(key) + len(value))
	if cdb.bufferedOffset > maxUint32 {
		return ErrTooMuchData
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
		tableEntries := cdb.entries[i]
		index[i] = table{
			offset: uint32(cdb.bufferedOffset),
			length: uint32(len(tableEntries)),
		}

		for _, entry := range tableEntries {
			err := writeTuple(cdb.bufferedWriter, entry.hash, entry.offset)
			if err != nil {
				return index, err
			}

			cdb.bufferedOffset += 8
			if cdb.bufferedOffset > maxUint32 {
				return index, ErrTooMuchData
			}
		}
	}

	// We're done with the buffer.
	err := cdb.bufferedWriter.Flush()
	cdb.bufferedWriter = nil
	if err != nil {
		return index, err
	}

	// Seek to the beginning of the file and write out the index.
	_, err = cdb.writer.Seek(0, os.SEEK_SET)
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
