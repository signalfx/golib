package boltcycle

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"

	"github.com/boltdb/bolt"
)

// CycleDB allows you to use a bolt.DB as a pseudo-LRU using a cycle of buckets
type CycleDB struct {
	// db is the bolt database values are stored into
	db *bolt.DB

	// bucketTimesIn is the name of the bucket we are putting our rotating values in
	bucketTimesIn []byte

	// minNumBuckets ensures you never delete an old bucket during a cycle if you have fewer than
	// these number of buckets
	minNumBuckets int
}

var errUnableToFindRootBucket = errors.New("unable to find root bucket")
var errUnexpectedBucketBytes = errors.New("bucket bytes not in uint64 form")
var errUnexpectedNonBucket = errors.New("unexpected non bucket")
var errNoLastBucket = errors.New("unable to find a last bucket")
var errOrderingWrong = errors.New("ordering wrong")

// KvPair is a pair of key/value that you want to write during a write call
type KvPair struct {
	// Key to write
	Key []byte
	// Value to write for key
	Value []byte
}

var defaultBucketName = []byte("cyc")

// Init a CycleDB to use a bolt database that cycles minNumBuckets buckets
func Init(db *bolt.DB, minNumBuckets int) (*CycleDB, error) {
	ret := &CycleDB{
		db:            db,
		bucketTimesIn: defaultBucketName,
		minNumBuckets: minNumBuckets,
	}
	err := ret.init()
	return ret, err
}

type stringCursor struct {
	cursor *bolt.Cursor
	head   string
}

type cursorHeap []stringCursor

func (c cursorHeap) Len() int {
	return len(c)
}

func (c cursorHeap) Less(i, j int) bool {
	return c[i].head < c[j].head
}

func (c cursorHeap) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c *cursorHeap) Push(x interface{}) {
	item := x.(stringCursor)
	*c = append(*c, item)
}

func (c *cursorHeap) Pop() interface{} {
	n := len(*c)
	item := (*c)[n-1]
	*c = (*c)[0 : n-1]
	return item
}

var _ heap.Interface = &cursorHeap{}

func (c *CycleDB) init() error {
	if c.db.IsReadOnly() {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(c.bucketTimesIn)
		if err != nil {
			return err
		}
		// If there is no bucket at all, make a first bucket at key=[0,0,0,0, 0,0,0,0]
		if k, _ := bucket.Cursor().First(); k == nil {
			var b [8]byte
			_, err := bucket.CreateBucket(b[:])
			return err
		}
		return nil
	})
}

// VerifyBuckets ensures that the cycle of buckets have the correct names (increasing 8 byte integers)
func (c *CycleDB) VerifyBuckets() error {
	return c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}

		// Each bucket should be 8 bytes of different uint64
		return bucket.ForEach(func(k, v []byte) error {
			if v != nil {
				return errUnexpectedNonBucket
			}
			if len(k) != 8 {
				return errUnexpectedBucketBytes
			}
			return nil
		})
	})
}

func createHeap(bucket *bolt.Bucket) (cursorHeap, error) {
	var ch cursorHeap
	// Each bucket should be 8 bytes of different uint64
	err := bucket.ForEach(func(k, v []byte) error {
		cursor := bucket.Bucket(k).Cursor()
		firstKey, _ := cursor.First()
		if firstKey != nil {
			ch = append(ch, stringCursor{cursor: cursor, head: string(firstKey)})
		}
		return nil
	})
	return ch, err
}

func verifyHeap(ch cursorHeap) error {
	top := ""
	heap.Init(&ch)
	for len(ch) > 0 {
		nextTop := ch[0].head
		if top != "" && nextTop <= top {
			return errOrderingWrong
		}
		top = nextTop
		headBytes, _ := ch[0].cursor.Next()
		if headBytes == nil {
			heap.Pop(&ch)
		} else {
			ch[0].head = string(headBytes)
			heap.Fix(&ch, 0)
		}
	}
	return nil
}

var createHeapFunc = createHeap

// VerifyCompressed checks that no key is repeated in the database
func (c *CycleDB) VerifyCompressed() error {
	return c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}

		ch, err := createHeapFunc(bucket)
		if err != nil {
			return err
		}
		return verifyHeap(ch)
	})
}

// CycleNodes deletes the first, oldest node in the primary bucket while there are >= minNumBuckets
// and creates a new, empty last node
func (c *CycleDB) CycleNodes() error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}

		countBuckets := func() int {
			num := 0
			cursor := bucket.Cursor()
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
				num++
			}
			return num
		}()

		cursor := bucket.Cursor()
		for k, _ := cursor.First(); k != nil && countBuckets > c.minNumBuckets; k, _ = cursor.Next() {
			if err := bucket.DeleteBucket(k); err != nil {
				return err
			}
			countBuckets--
		}

		lastBucket, _ := cursor.Last()
		nextBucketName := nextKey(lastBucket)
		_, err := bucket.CreateBucket(nextBucketName)

		return err
	})
}

func nextKey(last []byte) []byte {
	lastNum := binary.BigEndian.Uint64(last)
	var ret [8]byte
	binary.BigEndian.PutUint64(ret[:], lastNum+1)
	return ret[:]
}

type readToLocation struct {
	// bucket we found the []byte key in
	bucket uint64
	// Key we searched for
	key []byte
	// Value we found for the key, or nil of it wasn't found
	value []byte
	// needsCopy is true if we detected this item needs to be copied to the last bucket
	needsCopy bool
}

func (c *CycleDB) indexToLocation(toread [][]byte) ([]readToLocation, error) {
	res := make([]readToLocation, len(toread))

	indexesToFetch := make(map[int][]byte, len(toread))
	for i, bytes := range toread {
		indexesToFetch[i] = bytes
	}

	err := c.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}
		timeCursor := bucket.Cursor()
		needsCopy := false

		// We read values from the end to the start.  The last bucket is where we expect a read
		// heavy workload to have the key
		for lastKey, _ := timeCursor.Last(); lastKey != nil && len(indexesToFetch) > 0; lastKey, _ = timeCursor.Prev() {

			// All subkeys of our tree should be buckets
			timeBucket := bucket.Bucket(lastKey)
			if timeBucket == nil {
				return errUnexpectedNonBucket
			}
			bucketAsUint := binary.BigEndian.Uint64(lastKey)

			timeBucketCursor := timeBucket.Cursor()
			for index, searchBytes := range indexesToFetch {
				key, value := timeBucketCursor.Seek(searchBytes)
				if key == nil {
					continue
				}

				if bytes.Equal(key, searchBytes) {
					res[index].key = searchBytes
					res[index].value = make([]byte, len(value))
					copy(res[index].value, value)
					res[index].bucket = bucketAsUint
					res[index].needsCopy = needsCopy

					// We can remove this item since we don't need to search for it later
					delete(indexesToFetch, index)
				}
			}
			needsCopy = true
		}
		return nil
	})
	return res, err
}

func (c *CycleDB) moveRecentReads(readLocations []readToLocation, copyParts map[uint64][]int) error {
	if c.db.IsReadOnly() {
		return nil
	}
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}
		lastBucketKey, _ := bucket.Cursor().Last()
		if lastBucketKey == nil {
			return errNoLastBucket
		}
		lastBucket := bucket.Bucket(lastBucketKey)
		if lastBucket == nil {
			return errNoLastBucket
		}

		for bucketID, copyIndexes := range copyParts {
			var bucketName [8]byte
			binary.BigEndian.PutUint64(bucketName[:], bucketID)
			oldBucket := bucket.Bucket(bucketName[:])
			if oldBucket != nil {
				oldBucketCursor := oldBucket.Cursor()
				for _, rs := range copyIndexes {
					if err := cleanupBuckets(oldBucketCursor, lastBucket, readLocations[rs]); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}

var cleanupBuckets = cleanupBucketsFunc

func cleanupBucketsFunc(oldBucketCursor *bolt.Cursor, lastBucket *bolt.Bucket, readLoc readToLocation) error {
	k, _ := oldBucketCursor.Seek(readLoc.key)
	if bytes.Equal(k, readLoc.key) {
		if err := oldBucketCursor.Delete(); err != nil {
			return err
		}
	}
	return lastBucket.Put(readLoc.key, readLoc.value)
}

func (c *CycleDB) Read(toread [][]byte) ([][]byte, error) {
	readLocations, err := c.indexToLocation(toread)
	if err != nil {
		return nil, err
	}

	copyParts := make(map[uint64][]int, c.minNumBuckets+1)
	for i, readLocation := range readLocations {
		if readLocation.needsCopy {
			copyParts[readLocation.bucket] = append(copyParts[readLocation.bucket], i)
		}
	}

	if len(copyParts) > 0 {
		if err := c.moveRecentReads(readLocations, copyParts); err != nil {
			return nil, err
		}
	}

	res := make([][]byte, len(readLocations))
	for i, rl := range readLocations {
		res[i] = rl.value
	}
	return res, nil
}

func (c *CycleDB) Write(towrite []KvPair) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}
		lastBucketKey, _ := bucket.Cursor().Last()
		if lastBucketKey == nil {
			return errNoLastBucket
		}
		lastBucket := bucket.Bucket(lastBucketKey)
		if lastBucket == nil {
			return errNoLastBucket
		}
		for _, p := range towrite {
			if err := lastBucket.Put(p.Key, p.Value); err != nil {
				return err
			}
		}
		return nil
	})
}

// Delete all the keys from every bucket that could have the keys.  Returns true/false for each key
// if it exists
func (c *CycleDB) Delete(keys [][]byte) ([]bool, error) {
	ret := make([]bool, len(keys))
	return ret, c.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(c.bucketTimesIn)
		if bucket == nil {
			return errUnableToFindRootBucket
		}
		return bucket.ForEach(func(k, v []byte) error {
			innerBucket := bucket.Bucket(k)
			if innerBucket == nil {
				return errUnexpectedNonBucket
			}
			cursor := innerBucket.Cursor()
			return deleteKeys(keys, cursor, ret)
		})
	})
}

func deleteKeys(keys [][]byte, cursor *bolt.Cursor, ret []bool) error {
	for index, key := range keys {
		k, _ := cursor.Seek(key)
		if bytes.Equal(k, key) {
			if err := cursor.Delete(); err != nil {
				return err
			}
			ret[index] = true
		}
	}
	return nil
}
