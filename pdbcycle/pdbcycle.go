package pdbcycle

import (
	"bytes"
	"container/heap"
	"github.com/boltdb/bolt"
	"github.com/signalfx/golib/errors"
	"golang.org/x/net/context"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// CyclePDB allows you to use a bolt.DB as a pseudo-LRU using a cycle of buckets
type CyclePDB struct {
	// db is the bolt database values are stored into
	db *[]*bolt.DB

	dataDir string

	filenames *[]string

	diskTimeOut time.Duration

	// bucketTimesIn is the name of the bucket we are putting our rotating values in
	bucketTimesIn []byte

	// minNumOldBuckets ensures you never delete an old bucket during a cycle if you have fewer than
	// these number of buckets
	minNumOldBuckets int
	// Size of read moves to batch into a single transaction
	maxBatchSize int

	// Chan controls backlog of read moves
	readMovements chan readToLocation
	// How large the readMovements chan is when created
	readMovementBacklog int
	// log of errors
	asyncErrors chan<- error

	// wg controls waiting for the read movement loop
	wg sync.WaitGroup
	// stats records useful operation information for reporting back out by the user
	stats Stats

	// Stub functions used for testing
	cursorDelete func(*bolt.Cursor) error
}

// Stats are exported by CycleDB to let users inspect its behavior over time
type Stats struct {
	TotalItemsRecopied            int64
	TotalItemsAsyncPut            int64
	RecopyTransactionCount        int64
	TotalItemsDeletedDuringRecopy int64
	TotalReadCount                int64
	TotalWriteCount               int64
	TotalDeleteCount              int64
	TotalCycleCount               int64
	TotalErrorsDuringRecopy       int64
	TotalReadMovementsSkipped     int64
	TotalReadMovementsAdded       int64
	SizeOfBacklogToCopy           int
}

func (s *Stats) atomicClone() Stats {
	return Stats{
		TotalItemsRecopied:            atomic.LoadInt64(&s.TotalItemsRecopied),
		TotalItemsAsyncPut:            atomic.LoadInt64(&s.TotalItemsAsyncPut),
		RecopyTransactionCount:        atomic.LoadInt64(&s.RecopyTransactionCount),
		TotalItemsDeletedDuringRecopy: atomic.LoadInt64(&s.TotalItemsDeletedDuringRecopy),
		TotalReadCount:                atomic.LoadInt64(&s.TotalReadCount),
		TotalWriteCount:               atomic.LoadInt64(&s.TotalWriteCount),
		TotalDeleteCount:              atomic.LoadInt64(&s.TotalDeleteCount),
		TotalCycleCount:               atomic.LoadInt64(&s.TotalCycleCount),
		TotalErrorsDuringRecopy:       atomic.LoadInt64(&s.TotalErrorsDuringRecopy),
		TotalReadMovementsSkipped:     atomic.LoadInt64(&s.TotalReadMovementsSkipped),
		TotalReadMovementsAdded:       atomic.LoadInt64(&s.TotalReadMovementsAdded),
	}
}

var errorUnableToFindRootBucket = errors.New("unable to find root bucket")
var errorOrderingWrong = errors.New("ordering wrong")

// KvPair is a pair of key/value that you want to write during a write call
type KvPair struct {
	// Key to write
	Key []byte
	// Value to write for key
	Value []byte
}

var defaultBucketName = []byte("cyc")

// DBConfiguration are callbacks used as optional vardic parameters in New() to configure DB usage
type DBConfiguration func(*CyclePDB) error

// CycleLen sets the number of old buckets to keep around
func CycleLen(minNumOldBuckets int) DBConfiguration {
	return func(c *CyclePDB) error {
		c.minNumOldBuckets = minNumOldBuckets
		return nil
	}
}

// ReadMovementBacklog sets the size of the channel of read operations to rewrite
func ReadMovementBacklog(readMovementBacklog int) DBConfiguration {
	return func(c *CyclePDB) error {
		c.readMovementBacklog = readMovementBacklog
		return nil
	}
}

// AsyncErrors controls where we log async errors into.  If nil, they are silently dropped
func AsyncErrors(asyncErrors chan<- error) DBConfiguration {
	return func(c *CyclePDB) error {
		c.asyncErrors = asyncErrors
		return nil
	}
}

// BucketTimesIn is the sub bucket we put our cycled hashmap into
func BucketTimesIn(bucketName []byte) DBConfiguration {
	return func(c *CyclePDB) error {
		c.bucketTimesIn = bucketName
		return nil
	}
}

// New creates a CyclePDB to use a bolt database that cycles minNumOldBuckets buckets
func New(db *[]*bolt.DB, dataDir string, filenames *[]string, diskTimeOut time.Duration,
	optionalParameters ...DBConfiguration) (*CyclePDB, error) {
	ret := &CyclePDB{
		db:                  db,
		dataDir:             dataDir,
		filenames:           filenames,
		diskTimeOut:         diskTimeOut,
		bucketTimesIn:       defaultBucketName,
		minNumOldBuckets:    2,
		maxBatchSize:        1000,
		readMovementBacklog: 10000,
		cursorDelete: func(c *bolt.Cursor) error {
			return c.Delete()
		},
	}
	for i, config := range optionalParameters {
		if err := config(ret); err != nil {
			return nil, errors.Annotatef(err, "Cannot execute config parameter %d", i)
		}
	}
	if err := ret.init(); err != nil {
		return ret, errors.Annotate(err, "Cannot initialize database")
	}

	if !isReadOnly(ret) {
		ret.wg.Add(1)
		ret.readMovements = make(chan readToLocation, ret.readMovementBacklog)
		go ret.readMovementLoop()
	}
	return ret, nil
}

// Stats returns introspection stats about the Database.  The members are considered alpha and
// subject to change or rename.
func (c *CyclePDB) Stats() Stats {
	ret := c.stats.atomicClone()
	ret.SizeOfBacklogToCopy = len(c.readMovements)
	return ret
}

// Close ends the goroutine that moves read items to the latest bucket
func (c *CyclePDB) Close() error {
	if !isReadOnly(c) {
		close(c.readMovements)
	}
	c.wg.Wait()
	return nil
}

type kvHeap []string

func (kv kvHeap) Len() int {
	return len(kv)
}

func (kv kvHeap) Less(i, j int) bool {
	return kv[i] < kv[j]
}

func (kv kvHeap) Swap(i, j int) {
	kv[i], kv[j] = kv[j], kv[i]
}

func (kv *kvHeap) Push(x interface{}) {
	item := x.(string)
	*kv = append(*kv, item)
}

func (kv *kvHeap) Pop() interface{} {
	n := len(*kv)
	item := (*kv)[n-1]
	*kv = (*kv)[0 : n-1]
	return item
}

var _ heap.Interface = &kvHeap{}

func (c *CyclePDB) init() error {
	for i := range *c.db {
		if (*c.db)[i].IsReadOnly() {
			return nil
		}
		err := (*c.db)[i].Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists(c.bucketTimesIn)
			if err != nil {
				return errors.Annotatef(err, "Cannot find bucket %s", c.bucketTimesIn)
			}
			return err
		})

		if err != nil {
			return err
		}
	}
	return nil
}

// VerifyBuckets ensures that the cycle of buckets have the correct names (increasing 8 byte integers)
func (c *CyclePDB) VerifyBuckets() error {
	var err error

	for i := range *c.db {
		err = (*c.db)[i].View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(c.bucketTimesIn)
			if bucket == nil {
				return errorUnableToFindRootBucket
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return err
}

func createHeap(bucket *bolt.Bucket, kv *kvHeap) error {
	// Each bucket should be 8 bytes of different uint64

	err := bucket.ForEach(func(k, v []byte) error {
		if k != nil {
			*kv = append(*kv, string(k))
		}
		return nil
	})
	return errors.Annotate(err, "unable to create heap from bucket")
}

func verifyHeap(kv kvHeap) error {
	top := ""
	heap.Init(&kv)
	for len(kv) > 0 {
		nextTop := kv[0]
		if top != "" && nextTop <= top {
			return errorOrderingWrong
		}
		top = nextTop
		heap.Pop(&kv)
	}
	return nil
}

var createHeapFunc = createHeap

// VerifyCompressed checks that no key is repeated in the database
func (c *CyclePDB) VerifyCompressed() error {
	var kv kvHeap
	var err error
	for i := range *c.db {
		err = (*c.db)[i].View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(c.bucketTimesIn)
			if bucket == nil {
				return errorUnableToFindRootBucket
			}

			err := createHeapFunc(bucket, &kv)
			if err != nil {
				return errors.Annotate(err, "unable to create heap during compressed verification")
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	return verifyHeap(kv)
}

// CycleNodes deletes the first, oldest node in the primary bucket while there are >= minNumOldBuckets
// and creates a new, empty last node
func (c *CyclePDB) CycleNodes() error {
	atomic.AddInt64(&c.stats.TotalCycleCount, int64(1))
	var err error
	if len(*c.db) > c.minNumOldBuckets {
		fname := (*c.filenames)[0]
		*c.db = (*c.db)[1:]
		*c.filenames = (*c.filenames)[1:]
		err = os.Remove(fname)
		if err != nil {
			return err
		}
	}

	newFile := filepath.Join(c.dataDir, "PDB_"+strconv.FormatInt(time.Now().UTC().UnixNano(), 10)+".bolt")

	newdb, err := bolt.Open(newFile, os.FileMode(0666), &bolt.Options{
		Timeout: c.diskTimeOut,
	})

	if err != nil {
		return err
	}

	err = newdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(c.bucketTimesIn)
		if err != nil {
			return err
		}

		return nil
	})

	if err == nil {
		*c.db = append(*c.db, newdb)
		*c.filenames = append(*c.filenames, newFile)
	}

	return err
}

type readToLocation struct {
	// bucket we found the []byte key in
	dbndx int
	// Key we searched for
	key []byte
	// Value we found for the key, or nil of it wasn't found
	value []byte
	// needsCopy is true if we detected this item needs to be copied to the last bucket
	needsCopy bool
}

func (c *CyclePDB) readMovementLoop() {
	defer c.wg.Done()
	for {
		allMovements := drainAllMovements(c.readMovements, c.maxBatchSize)
		if allMovements == nil {
			return
		}
		if err := c.moveRecentReads(allMovements); err != nil {
			atomic.AddInt64(&c.stats.TotalErrorsDuringRecopy, 1)
			if c.asyncErrors != nil {
				c.asyncErrors <- err
			}
		}
	}
}

func drainAllMovements(readMovements <-chan readToLocation, maxBatchSize int) []readToLocation {
	allMovements := make([]readToLocation, 0, maxBatchSize)
	var rm readToLocation
	var ok bool
	if rm, ok = <-readMovements; !ok {
		return nil
	}
	allMovements = append(allMovements, rm)

	for len(allMovements) < maxBatchSize {
		select {
		case rm, ok := <-readMovements:
			if !ok {
				return allMovements
			}
			allMovements = append(allMovements, rm)
		default:
			return allMovements
		}
	}
	return allMovements
}

func (c *CyclePDB) indexToLocation(toread [][]byte) ([]readToLocation, error) {
	res := make([]readToLocation, len(toread))
	var err error
	needsCopy := false

	indexesToFetch := make(map[int][]byte, len(toread))
	for i, bytes := range toread {
		indexesToFetch[i] = bytes
	}

	for i := range *c.db {
		err = (*c.db)[len(*c.db)-1-i].View(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(c.bucketTimesIn)
			if bucket == nil {
				return errorUnableToFindRootBucket
			}

			// We read values from the end to the start.  The last bucket is where we expect a read
			// heavy workload to have the key

			timeBucketCursor := bucket.Cursor()
			for index, searchBytes := range indexesToFetch {
				key, value := timeBucketCursor.Seek(searchBytes)
				if key == nil {
					continue
				}

				if bytes.Equal(key, searchBytes) {
					res[index].key = searchBytes
					res[index].value = make([]byte, len(value))
					// Note: The returned value is only valid for the lifetime of the transaction so
					//       we must copy it out
					copy(res[index].value, value)
					res[index].dbndx = len(*c.db) - 1 - i
					res[index].needsCopy = needsCopy

					// We can remove this item since we don't need to search for it later
					delete(indexesToFetch, index)
				}
			}
			needsCopy = true
			return nil
		})
		if err != nil {
			return res, errors.Annotate(err, "cannot finish database view function")
		}
	}
	return res, errors.Annotate(err, "cannot finish database view function")
}

func (c *CyclePDB) moveRecentReads(readLocations []readToLocation) error {
	dbndxToReadLocations := make(map[int][]readToLocation)
	for _, r := range readLocations {
		dbndxToReadLocations[r.dbndx] = append(dbndxToReadLocations[r.dbndx], r)
	}

	return (*c.db)[len(*c.db)-1].Update(func(tx *bolt.Tx) error {
		atomic.AddInt64(&c.stats.RecopyTransactionCount, int64(1))
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errorUnableToFindRootBucket
		}

		recopyCount := int64(0)
		deletedCount := int64(0)
		asyncPutCount := int64(0)

		for dbndx, readLocs := range dbndxToReadLocations {
			if dbndx == len(*c.db)-1 {
				continue
			} else {
				if err := deleteFromOldBucket((*c.db)[dbndx], c.bucketTimesIn, readLocs, c.cursorDelete, &recopyCount, &deletedCount); err != nil {
					return errors.Annotate(err, "cannot remove keys from the old bucket")
				}
			}

			for _, rs := range readLocs {
				asyncPutCount++
				if err := bucket.Put(rs.key, rs.value); err != nil {
					return errors.Annotate(err, "cannot puts keys into the new bucket")
				}
			}
		}
		atomic.AddInt64(&c.stats.TotalItemsRecopied, recopyCount)
		atomic.AddInt64(&c.stats.TotalItemsAsyncPut, asyncPutCount)
		atomic.AddInt64(&c.stats.TotalItemsDeletedDuringRecopy, deletedCount)
		return nil
	})
}

func deleteFromOldBucket(db *bolt.DB, bucketName []byte, readLocs []readToLocation, cursorDelete func(*bolt.Cursor) error, recopyCount *int64, deletedCount *int64) error {

	return db.Update(func(tx *bolt.Tx) error {
		oldBucket := tx.Bucket(bucketName)
		if oldBucket != nil {
			oldBucketCursor := oldBucket.Cursor()
			for _, rs := range readLocs {
				k, _ := oldBucketCursor.Seek(rs.key)
				*recopyCount++
				if k != nil && bytes.Equal(k, rs.key) {
					if err := cursorDelete(oldBucketCursor); err != nil {
						return errors.Annotatef(err, "cannot delete key %v", rs.key)
					}
					*deletedCount++
				}
			}
		}
		return nil
	})
}

func isReadOnly(c *CyclePDB) bool {
	for i := range *c.db {
		if (*c.db)[i].IsReadOnly() {
			return true
		}
	}
	return false
}

//// Read bytes from the first available bucket.  Do not modify the returned bytes because
//// they are recopied to later cycle databases if needed.
func (c *CyclePDB) Read(toread [][]byte) ([][]byte, error) {
	atomic.AddInt64(&c.stats.TotalReadCount, int64(len(toread)))
	readLocations, err := c.indexToLocation(toread)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to convert indexes to read location")
	}

	if !isReadOnly(c) {
		skips := int64(0)
		adds := int64(0)
		for _, readLocation := range readLocations {
			if readLocation.needsCopy {
				select {
				case c.readMovements <- readLocation:
					adds++
				default:
					skips++
				}
			}
		}
		if skips != 0 {
			atomic.AddInt64(&c.stats.TotalReadMovementsSkipped, skips)
		}
		if adds != 0 {
			atomic.AddInt64(&c.stats.TotalReadMovementsAdded, adds)
		}
	}

	res := make([][]byte, len(readLocations))
	for i, rl := range readLocations {
		res[i] = rl.value
	}
	return res, nil
}

// AsyncWrite will enqueue a write into the same chan that moves reads to the last bucket.  You
// must not *ever* change the []byte given to towrite since you can't know when that []byte is
// finished being used.  Note that if the readMovements queue is backed up this operation will block
// until it has room.
func (c *CyclePDB) AsyncWrite(ctx context.Context, towrite []KvPair) {
	for _, w := range towrite {
		select {
		case <-ctx.Done():
			return
		case c.readMovements <- readToLocation{
			key:   w.Key,
			value: w.Value,
		}:
		}
	}
}

// Write a pair of key/value items into the cycle disk
func (c *CyclePDB) Write(towrite []KvPair) error {
	atomic.AddInt64(&c.stats.TotalWriteCount, int64(len(towrite)))

	return (*c.db)[len(*c.db)-1].Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errorUnableToFindRootBucket
		}

		for _, p := range towrite {
			if err := bucket.Put(p.Key, p.Value); err != nil {
				return errors.Annotatef(err, "cannot put for key %v", p.Key)
			}
		}
		return nil
	})
}

// Delete all the keys from every bucket that could have the keys.  Returns true/false for each key
// if it exists
func (c *CyclePDB) Delete(keys [][]byte) ([]bool, error) {
	atomic.AddInt64(&c.stats.TotalDeleteCount, int64(len(keys)))
	ret := make([]bool, len(keys))
	var err error

	for i := range *c.db {
		err = (*c.db)[i].Update(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(c.bucketTimesIn)
			if bucket == nil {
				return errorUnableToFindRootBucket
			}
			cursor := bucket.Cursor()
			return deleteKeys(keys, cursor, ret)
		})
		if err != nil {
			return ret, err
		}
	}
	return ret, err
}

func deleteKeys(keys [][]byte, cursor *bolt.Cursor, ret []bool) error {
	for index, key := range keys {
		k, _ := cursor.Seek(key)
		if bytes.Equal(k, key) {
			if err := cursor.Delete(); err != nil {
				return errors.Annotatef(err, "cannot delete key %v", k)
			}
			ret[index] = true
		}
	}
	return nil
}
