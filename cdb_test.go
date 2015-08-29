package cdb

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var expectedRecords = [][][]byte{
	{[]byte("foo"), []byte("bar")},
	{[]byte("baz"), []byte("quuuux")},
	{[]byte("playwright"), []byte("wow")},
	{[]byte("crystal"), []byte("CASTLES")},
	{[]byte("CRYSTAL"), []byte("castles")},
	{[]byte("snush"), []byte("collision!")}, // 'playwright' collides with 'snush' in cdbhash
	{[]byte("a"), []byte("a")},
	{[]byte(""), []byte("empty_key")},
	{[]byte("empty_value"), []byte("")},
	{[]byte("not in the table"), nil},
}

func TestGet(t *testing.T) {
	db, err := Open("./test/test.cdb")
	require.NoError(t, err)
	require.NotNil(t, db)

	records := append(append(expectedRecords, expectedRecords...), expectedRecords...)
	shuffle(records)

	for _, record := range records {
		msg := "while fetching " + string(record[0])

		value, err := db.Get(record[0])
		require.NoError(t, err, msg)
		assert.Equal(t, record[1], value, msg)
	}
}

func BenchmarkGet(b *testing.B) {
	db, _ := Open("./test/test.cdb")
	b.ResetTimer()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		record := expectedRecords[rand.Intn(len(expectedRecords))]
		db.Get(record[0])
	}
}

func shuffle(a [][][]byte) {
	rand.Seed(time.Now().UnixNano())
	for i := range a {
		j := rand.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}
