package cdb_test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"testing/quick"
	"time"
	"strconv"

	"github.com/colinmarc/cdb"
	"github.com/Pallinder/go-randomdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWritesReadable(t *testing.T) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	require.NoError(t, err)
	require.NotNil(t, writer)

	expected := make([][][]byte, 0, 100)
	for i := 0; i < cap(expected); i++ {
		key := []byte(strconv.Itoa(i))
		value := []byte(randomdata.SillyName())
		err := writer.Put(key, value)
		require.NoError(t, err)

		expected = append(expected, [][]byte{key, value})
	}

	db, err := writer.Freeze()
	require.NoError(t, err)

	for _, record := range expected {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		require.Nil(t, err)
		assert.Equal(t, string(record[1]), string(val), msg)
	}
}

func TestWritesRandom(t *testing.T) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	writer, err := cdb.NewWriter(f)
	require.NoError(t, err)
	require.NotNil(t, writer)

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	records := make([][][]byte, 0, 1000)
	seenKeys := make(map[string]bool)
	stringType := reflect.TypeOf("")

	// Make sure we don't end up with duplicate keys, since that makes testing
	// hard.
	for len(records) < cap(records) {
		key, _ := quick.Value(stringType, random)
		if !seenKeys[key.String()] {
			value, _ := quick.Value(stringType, random)
			keyBytes := []byte(key.String())
			valueBytes := []byte(value.String())
			records = append(records, [][]byte{keyBytes, valueBytes})
			seenKeys[key.String()] = true
		}
	}

	for _, record := range records {
		err := writer.Put(record[0], record[1])
		require.NoError(t, err)
	}

	db, err := writer.Freeze()
	require.NoError(t, err)

	for _, record := range records {
		msg := "while fetching " + string(record[0])
		val, err := db.Get(record[0])
		require.Nil(t, err)
		assert.Equal(t, string(record[1]), string(val), msg)
	}
}

func BenchmarkPut(b *testing.B) {
	f, err := ioutil.TempFile("", "test-cdb")
	require.NoError(b, err)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()

	writer, err := cdb.NewWriter(f)
	require.NoError(b, err)

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	stringType := reflect.TypeOf("")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key, _ := quick.Value(stringType, random)
		value, _ := quick.Value(stringType, random)
		keyBytes := []byte(key.String())
		valueBytes := []byte(value.String())

		writer.Put(keyBytes, valueBytes)
	}
}
