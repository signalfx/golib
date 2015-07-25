package boltcycle

import (
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/boltdb/bolt"
	"github.com/stretchr/testify/require"
)

type testSetup struct {
	filename string
	cdb      *CycleDB
	cycleLen int
	t        *testing.T
	readOnly bool
}

func (t *testSetup) Errorf(format string, args ...interface{}) {
	t.t.Errorf(format, args...)
}

func (t *testSetup) FailNow() {
	buf := make([]byte, 1024)
	runtime.Stack(buf, false)
	t.t.Errorf("%s\n", string(buf))
	t.t.FailNow()
}

func (t *testSetup) Close() error {
	require.NoError(t, os.Remove(t.filename))
	return nil
}

func setupCdb(t *testing.T, cycleLen int) testSetup {
	f, err := ioutil.TempFile("", "TestDiskCache")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	require.NoError(t, os.Remove(f.Name()))
	ret := testSetup{
		filename: f.Name(),
		cycleLen: cycleLen,
		t:        t,
	}
	ret.canOpen()
	return ret
}

func (t *testSetup) canOpen() {
	db, err := bolt.Open(t.filename, os.FileMode(0666), &bolt.Options{
		ReadOnly: t.readOnly,
		Timeout:  time.Second,
	})
	require.NoError(t, err)

	t.cdb, err = Init(db, t.cycleLen)
	require.NoError(t, err)
	require.NoError(t, t.cdb.VerifyCompressed())
}

func (t *testSetup) isEmpty(key string) {
	r, err := t.cdb.Read([][]byte{[]byte(key)})
	require.NoError(t, err)
	require.Equal(t, [][]byte{nil}, r)
}

func (t *testSetup) canWrite(key string, value string) {
	require.NoError(t, t.cdb.Write([]KvPair{{[]byte(key), []byte(value)}}))
}

func (t *testSetup) canDelete(key string, exists bool) {
	e, err := t.cdb.Delete([][]byte{[]byte(key)})
	require.NoError(t, err)
	require.Equal(t, e[0], exists)
}

func (t *testSetup) equals(key string, value string) {
	r, err := t.cdb.Read([][]byte{[]byte(key)})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(value)}, r)
}

func (t *testSetup) canCycle() {
	require.NoError(t, t.cdb.CycleNodes())
}

func (t *testSetup) canClose() {
	require.NoError(t, t.cdb.db.Close())
}

func (t *testSetup) isVerified() {
	require.NoError(t, t.cdb.VerifyBuckets())
}

func (t *testSetup) isCompressed() {
	require.NoError(t, t.cdb.VerifyCompressed())
}

func TestCursorHeap(t *testing.T) {
	c := cursorHeap([]stringCursor{})
	c.Push(stringCursor{head: "a"})
	c.Push(stringCursor{head: "b"})
	require.True(t, c.Less(0, 1))
}

func TestErrUnexpectedNonBucket(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	require.NoError(t, testRun.cdb.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(testRun.cdb.bucketTimesIn).Put([]byte("hi"), []byte("world"))
	}))
	require.Equal(t, errUnexpectedNonBucket, testRun.cdb.VerifyBuckets())
	_, err := testRun.cdb.Delete([][]byte{[]byte("hello")})
	require.Equal(t, errUnexpectedNonBucket, err)

	_, err = testRun.cdb.Read([][]byte{[]byte("hello")})
	require.Equal(t, errUnexpectedNonBucket, err)
}

func TestErrUnexpectedBucketBytes(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	require.NoError(t, testRun.cdb.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.Bucket(testRun.cdb.bucketTimesIn).CreateBucket([]byte("_"))
		return err
	}))
	require.Equal(t, errUnexpectedBucketBytes, testRun.cdb.VerifyBuckets())
}

func TestVerifyCompressed(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.canWrite("hello", "world")
	testRun.canWrite("hello2", "world")
	testRun.canCycle()
	testRun.canWrite("hello", "world")

	require.Equal(t, errOrderingWrong, testRun.cdb.VerifyCompressed())

	e1 := errors.New("nope")
	createHeapFunc = func(*bolt.Bucket) (cursorHeap, error) {
		return nil, e1
	}

	require.Equal(t, e1, testRun.cdb.VerifyCompressed())

	createHeapFunc = createHeap
}

func TestErrNoLastBucket(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.cdb.bucketTimesIn = []byte("empty_bucket")
	require.NoError(t, testRun.cdb.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucket(testRun.cdb.bucketTimesIn)
		b.Put([]byte("hello"), []byte("invalid_setup"))
		return err
	}))
	require.Equal(t, errNoLastBucket, testRun.cdb.moveRecentReads(nil, nil))
	require.Equal(t, errNoLastBucket, testRun.cdb.Write([]KvPair{}))

	require.NoError(t, testRun.cdb.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(testRun.cdb.bucketTimesIn)
		return b.Delete([]byte("hello"))
	}))
	require.Equal(t, errNoLastBucket, testRun.cdb.moveRecentReads(nil, nil))
	require.Equal(t, errNoLastBucket, testRun.cdb.Write([]KvPair{}))
}

func TestMoveRecentReads(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.canWrite("hello", "world")
	testRun.canCycle()
	testRun.canClose()
	testRun.readOnly = true
	testRun.canOpen()
	testRun.equals("hello", "world")
}

func TestBadCleanup(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.canWrite("hello", "world")
	testRun.canCycle()
	e1 := errors.New("nope")
	cleanupBuckets = func(oldBucketCursor *bolt.Cursor, lastBucket *bolt.Bucket, readLoc readToLocation) error {
		return e1
	}
	defer func() {
		cleanupBuckets = cleanupBucketsFunc
	}()
	_, err := testRun.cdb.Read([][]byte{[]byte("hello")})
	require.Equal(t, e1, err)

	err = testRun.cdb.db.View(func(tx *bolt.Tx) error {
		var bucketName [8]byte
		binary.BigEndian.PutUint64(bucketName[:], 0)
		cur := tx.Bucket(testRun.cdb.bucketTimesIn).Bucket(bucketName[:]).Cursor()
		return cleanupBucketsFunc(cur, nil, readToLocation{key: []byte("hello")})
	})
	require.Equal(t, bolt.ErrTxNotWritable, err)
}

func TestErrOnWrite(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	require.Error(t, testRun.cdb.Write([]KvPair{{}}))
}

func TestErrOnDelete(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.canWrite("hello", "world")
	testRun.canCycle()

	err := testRun.cdb.db.View(func(tx *bolt.Tx) error {
		var bucketName [8]byte
		binary.BigEndian.PutUint64(bucketName[:], 0)
		cur := tx.Bucket(testRun.cdb.bucketTimesIn).Bucket(bucketName[:]).Cursor()
		return deleteKeys([][]byte{[]byte("hello")}, cur, []bool{false})
	})
	require.Equal(t, bolt.ErrTxNotWritable, err)
}

func TestErrUnableToFindRootBucket(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.cdb.bucketTimesIn = []byte("not_here")
	require.Equal(t, errUnableToFindRootBucket, testRun.cdb.VerifyBuckets())
	require.Equal(t, errUnableToFindRootBucket, testRun.cdb.VerifyCompressed())
	require.Equal(t, errUnableToFindRootBucket, testRun.cdb.CycleNodes())
	require.Equal(t, errUnableToFindRootBucket, testRun.cdb.moveRecentReads(nil, nil))

	_, err := testRun.cdb.Read([][]byte{[]byte("hello")})
	require.Equal(t, errUnableToFindRootBucket, err)

	err = testRun.cdb.Write([]KvPair{{[]byte("hello"), []byte("world")}})
	require.Equal(t, errUnableToFindRootBucket, err)

	_, err = testRun.cdb.Delete([][]byte{[]byte("hello")})
	require.Equal(t, errUnableToFindRootBucket, err)
}

func TestDatabaseInit(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.canClose()
	testRun.canOpen()
	testRun.cdb.bucketTimesIn = []byte{}
	require.Error(t, testRun.cdb.init())
}

func TestCycleNodes(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.cdb.minNumBuckets = 0
	require.NoError(t, testRun.cdb.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(testRun.cdb.bucketTimesIn).Put([]byte("hi"), []byte("world"))
	}))
	require.Equal(t, bolt.ErrIncompatibleValue, testRun.cdb.CycleNodes())
}

func TestDatabaseCycle(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()
	testRun.isVerified()

	testRun.isEmpty("hello")
	testRun.canCycle()
	testRun.canWrite("hello", "world")
	testRun.isCompressed()
	testRun.equals("hello", "world")

	testRun.canCycle()
	testRun.equals("hello", "world")

	for i := 0; i < testRun.cycleLen; i++ {
		testRun.canCycle()
		testRun.isCompressed()
	}

	testRun.equals("hello", "world")

	for i := 0; i < testRun.cycleLen+1; i++ {
		testRun.canCycle()
		testRun.isCompressed()
	}

	testRun.isEmpty("hello")
	testRun.isCompressed()
}

func TestReadDelete(t *testing.T) {
	testRun := setupCdb(t, 5)
	defer testRun.Close()

	testRun.isEmpty("hello")
	testRun.canDelete("hello", false)
	testRun.isEmpty("hello")

	testRun.canWrite("hello", "world")
	testRun.equals("hello", "world")

	testRun.canDelete("hello", true)
	testRun.canDelete("hello", false)
	testRun.isEmpty("hello")

	testRun.canWrite("hello", "world")
	testRun.canCycle()
	testRun.canDelete("hello", true)
	testRun.isEmpty("hello")
}
