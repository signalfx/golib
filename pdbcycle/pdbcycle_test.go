package pdbcycle

import (
	"io/ioutil"
	"os"
	"runtime"
	"testing"
	"time"
	"github.com/signalfx/golib/errors"
	"bytes"
	"github.com/boltdb/bolt"
	"github.com/signalfx/golib/log"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
	"strconv"
)

type testSetup struct {
	filenames            *[]string
	cpdb                 *CyclePDB
	cycleLen             int
	readMovementsBacklog int
	log                  bytes.Buffer
	t                    *testing.T
	failedDelete         bool
	asyncErrors          chan error
	readOnly             bool
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
	require.NoError(t, t.cpdb.Close())
	for i := range *t.cpdb.db {
		require.NoError(t, (*t.cpdb.db)[i].Close())
		require.NoError(t, os.Remove((*t.cpdb.filenames)[i]))
	}
	return nil
}

func setupCpdb(t *testing.T, cycleLen int) testSetup {
	f1, err := ioutil.TempFile("", "PDB_"+strconv.FormatInt(time.Now().UTC().UnixNano(), 10) + ".bolt")
	f := make([]string, 0)
	f = append(f, f1.Name())
	require.NoError(t, err)
	require.NoError(t, f1.Close())
	require.NoError(t, os.Remove(f1.Name()))
	ret := testSetup{
		filenames:            &f,
		cycleLen:             cycleLen,
		t:                    t,
		readMovementsBacklog: 10,
		asyncErrors:          make(chan error),
	}
	ret.canOpen()
	return ret
}

func (t *testSetup) canOpen() {
	dbqueue := make([]*bolt.DB, 0)
	var err error
	var db *bolt.DB

	for _, f := range *t.filenames {
		db, err = bolt.Open(f, os.FileMode(0666), &bolt.Options{
			ReadOnly: t.readOnly,
			Timeout:  time.Second,
		})
		dbqueue = append(dbqueue, db)
		require.NoError(t, err)
	}

	args := []DBConfiguration{CycleLen(t.cycleLen), ReadMovementBacklog(t.readMovementsBacklog), AsyncErrors(t.asyncErrors)}
	if t.failedDelete {
		args = append(args, func(c *CyclePDB) error {
			c.cursorDelete = func(*bolt.Cursor) error {
				return errors.New("nope")
			}
			return nil
		})
	}
	t.cpdb, err = New(&dbqueue, t.filenames, time.Second, args...)
	require.NoError(t, err)
	require.NoError(t, t.cpdb.VerifyCompressed())
}

func (t *testSetup) isEmpty(key string) {
	r, err := t.cpdb.Read([][]byte{[]byte(key)})
	require.NoError(t, err)
	require.Equal(t, [][]byte{nil}, r)
}

func (t *testSetup) canWrite(key string, value string) {
	require.NoError(t, t.cpdb.Write([]KvPair{{[]byte(key), []byte(value)}}))
}

func (t *testSetup) canDelete(key string, exists bool) {
	e, err := t.cpdb.Delete([][]byte{[]byte(key)})
	require.NoError(t, err)
	require.Equal(t, e[0], exists)
}

func (t *testSetup) equals(key string, value string) {
	r, err := t.cpdb.Read([][]byte{[]byte(key)})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(value)}, r)
}

func (t *testSetup) canCycle() {
	require.NoError(t, t.cpdb.CycleNodes())
}

func (t *testSetup) canClose() {
	for i := range *t.cpdb.db {
		require.NoError(t, (*t.cpdb.db)[i].Close())
	}
}

func (t *testSetup) isVerified() {
	require.NoError(t, t.cpdb.VerifyBuckets())
}

func (t *testSetup) isCompressed() {
	require.NoError(t, t.cpdb.VerifyCompressed())
}

func TestKVHeap(t *testing.T) {
	kv := kvHeap([]string{})
	kv.Push("a")
	kv.Push("b")
	require.True(t, kv.Less(0, 1))
}

func TestVerifyCompressed(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.canWrite("hello", "world")
	testRun.canWrite("hello2", "world")
	testRun.canCycle()
	testRun.canWrite("hello", "world")

	require.Equal(t, errOrderingWrong, testRun.cpdb.VerifyCompressed())

	e1 := errors.New("nope")
	createHeapFunc = func(*bolt.Bucket, *kvHeap) error {
		return e1
	}

	require.Equal(t, e1, errors.Tail(testRun.cpdb.VerifyCompressed()))

	createHeapFunc = createHeap
}

func TestMoveRecentReads(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.canWrite("hello", "world")
	testRun.canCycle()
	testRun.canClose()
	testRun.readOnly = true
	testRun.canOpen()
	testRun.equals("hello", "world")
}

func TestAsyncWriteEventuallyHappens(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.cpdb.AsyncWrite(context.Background(), []KvPair{{Key: []byte("hello"), Value: []byte("world")}})
	for testRun.cpdb.Stats().TotalItemsAsyncPut == 0 {
		runtime.Gosched()
	}
	// No assert needed.  Testing that write eventually happens
}

func TestAsyncWriteBadDelete(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.canClose()
	testRun.failedDelete = true
	testRun.canOpen()
	testRun.canWrite("hello", "world")
	testRun.canCycle()
	testRun.equals("hello", "world")
	e := <-testRun.asyncErrors
	require.Equal(t, "nope", e.Error())
}

func TestBoltCycleRead(t *testing.T) {
	Convey("when setup to fail", t, func() {
		testRun := setupCpdb(t, 5)
		log.IfErr(log.Panic, testRun.Close())
		testRun.readMovementsBacklog = 0
		testRun.failedDelete = true
		testRun.canOpen()
		testRun.canWrite("hello", "world")
		So(testRun.cpdb.stats.TotalReadMovementsSkipped, ShouldEqual, 0)
		testRun.canCycle()
		Convey("and channel is full because error is blocking", func() {
			testRun.equals("hello", "world")
			Convey("future reads should not block", func() {
				// This should not block
				testRun.equals("hello", "world")
				So(testRun.cpdb.stats.TotalReadMovementsSkipped, ShouldBeGreaterThan, 0)
			})
		})
		Reset(func() {
			go func() {
				<-testRun.asyncErrors
			}()

			log.IfErr(log.Panic, testRun.Close())
			close(testRun.asyncErrors)
		})
	})
}

func TestAsyncWrite(t *testing.T) {
	Convey("when setup", t, func() {
		c := CyclePDB{}
		Convey("and channel is full", func() {
			c.readMovements = make(chan readToLocation)
			Convey("Async write should timeout", func() {
				ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
				c.AsyncWrite(ctx, []KvPair{{}})
			})
		})
	})
}

func TestAsyncWriteBadValue(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.cpdb.AsyncWrite(context.Background(), []KvPair{{Key: []byte(""), Value: []byte("world")}})
	require.Equal(t, bolt.ErrKeyRequired, errors.Tail(<-testRun.asyncErrors))
}

func TestErrOnWrite(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	require.Error(t, testRun.cpdb.Write([]KvPair{{}}))
}

func TestErrUnableToFindRootBucket(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.cpdb.bucketTimesIn = []byte("not_here")
	require.Equal(t, errUnableToFindRootBucket, testRun.cpdb.VerifyBuckets())
	require.Equal(t, errUnableToFindRootBucket, testRun.cpdb.VerifyCompressed())
	require.Equal(t, errUnableToFindRootBucket, testRun.cpdb.moveRecentReads(nil))

	_, err := testRun.cpdb.Read([][]byte{[]byte("hello")})
	require.Equal(t, errUnableToFindRootBucket, errors.Tail(err))

	err = testRun.cpdb.Write([]KvPair{{[]byte("hello"), []byte("world")}})
	require.Equal(t, errUnableToFindRootBucket, errors.Tail(err))

	_, err = testRun.cpdb.Delete([][]byte{[]byte("hello")})
	require.Equal(t, errUnableToFindRootBucket, errors.Tail(err))
}

func TestDatabaseInit(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.canClose()
	testRun.canOpen()
	_, err := New(testRun.cpdb.db, testRun.cpdb.filenames, time.Second, BucketTimesIn([]byte{}))
	require.Error(t, err)
}


func TestDatabaseCycle(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()
	testRun.isVerified()

	testRun.isEmpty("hello")
	testRun.canCycle()
	testRun.canWrite("hello", "world")
	testRun.isCompressed()
	testRun.equals("hello", "world")

	startingMoveCount := testRun.cpdb.Stats().TotalItemsRecopied
	testRun.canCycle()
	testRun.equals("hello", "world")
	// Give "hello" time to copy to the last bucket
	for startingMoveCount == testRun.cpdb.Stats().TotalItemsRecopied {
		runtime.Gosched()
	}

	for i := 0; i < testRun.cycleLen; i++ {
		testRun.canCycle()
		testRun.isCompressed()
	}

	begin := testRun.cpdb.Stats().TotalItemsRecopied
	testRun.equals("hello", "world")
	for testRun.cpdb.Stats().TotalItemsRecopied == begin {
		runtime.Gosched()
	}

	for testRun.cpdb.Stats().SizeOfBacklogToCopy > 0 {
		runtime.Gosched()
	}

	for i := 0; i < testRun.cycleLen+1; i++ {
		testRun.canCycle()
		testRun.isCompressed()
	}

	testRun.isEmpty("hello")
	testRun.isCompressed()
}

func TestReadDelete(t *testing.T) {
	testRun := setupCpdb(t, 5)
	defer func() {
		log.IfErr(log.Panic, testRun.Close())
	}()

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

func TestBadInit(t *testing.T) {
	expected := errors.New("nope")
	_, err := New(nil, nil, time.Second, func(*CyclePDB) error { return expected })
	require.Equal(t, expected, errors.Tail(err))
}

func TestDrainAllMovements(t *testing.T) {
	{
		readMovements := make(chan readToLocation)
		maxBatchSize := 10
		close(readMovements)
		n := drainAllMovements(readMovements, maxBatchSize)
		require.Nil(t, n)
	}
	{
		readMovements := make(chan readToLocation, 3)
		maxBatchSize := 10
		readMovements <- readToLocation{dbndx: 0}
		close(readMovements)
		n := drainAllMovements(readMovements, maxBatchSize)
		require.Equal(t, []readToLocation{{dbndx: 0}}, n)
	}
	{
		readMovements := make(chan readToLocation, 3)
		maxBatchSize := 2
		readMovements <- readToLocation{dbndx: 0}
		readMovements <- readToLocation{dbndx: 1}
		close(readMovements)
		n := drainAllMovements(readMovements, maxBatchSize)
		require.Equal(t, []readToLocation{{dbndx: 0}, {dbndx: 1}}, n)
	}
}
