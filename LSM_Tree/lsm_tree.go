package lsmtree

import (
	"sync"
	"time"
)

const (
	MaximumElement      = 1024
	CompactionFrequency = 1000
	BloomErrorRate      = 0.001
	BloomFilterCapacity = 1000000
)

type KV struct {
	Key       string
	Value     string
	Tombstone bool
}

type LSMTree struct {
	treeRWLock     sync.RWMutex
	diskRWLock     sync.RWMutex
	tree           *Node
	secondaryTree  *Node
	diskFiles      []DiskFile
	flushThreshold int
	BloomFilter    *CustomBloomFilter
}

type LSMTreeOptions struct {
	MaximumElement     int
	CompactionPeriod   int
	BloomFilterOptions CustomBloomFilterOptions
}

func InitLsmTree(options LSMTreeOptions) *LSMTree {
	lsmTree := &LSMTree{
		tree:           &Node{},
		secondaryTree:  &Node{},
		diskFiles:      []DiskFile{},
		flushThreshold: options.MaximumElement,
		BloomFilter:    NewCustomBloomFilter(options.BloomFilterOptions),
	}

	go lsmTree.PeriodicCompaction(options.CompactionPeriod)
	return lsmTree
}

func (lsmTree *LSMTree) PeriodicCompaction(CompactionPeriod int) {

	for {
		time.Sleep(time.Duration(CompactionPeriod) * time.Millisecond)

		var db1, db2 DiskFile

		lsmTree.diskRWLock.RLock()

		if len(lsmTree.diskFiles) >= 2 {
			db1 = lsmTree.diskFiles[len(lsmTree.diskFiles)-1]
			db2 = lsmTree.diskFiles[len(lsmTree.diskFiles)-2]
		}

		if db1.Empty() || db2.Empty() {

			continue
		}

		newDiskBlock := compact(db1, db2)

		lsmTree.diskFiles = lsmTree.diskFiles[0 : len(lsmTree.diskFiles)-2]
		lsmTree.diskFiles = append(lsmTree.diskFiles, newDiskBlock)
		lsmTree.diskRWLock.RUnlock()

	}
}

func compact(db1 DiskFile, db2 DiskFile) DiskFile {
	pairs1 := db1.All()
	pairs2 := db2.All()

	// merge the two arrays in the increasing order of key values
	i, j := 0, 0
	var newPairs []KV

	for i < len(pairs1) && j < len(pairs2) {
		if pairs1[i].Key < pairs2[j].Key {
			newPairs = append(newPairs, pairs1[i])
			i++
		} else {
			newPairs = append(newPairs, pairs2[j])
			j++
		}
	}

	for i < len(pairs1) {
		newPairs = append(newPairs, pairs1[i])
		i++
	}

	for j < len(pairs2) {
		newPairs = append(newPairs, pairs2[j])
		j++
	}

	return NewDiskFile(newPairs)

}

func (lsmTree *LSMTree) Get(key string) (string, bool) {

	lsmTree.treeRWLock.RLock()

	pair, err := lsmTree.tree.Find(key)

	if err == nil {

		lsmTree.treeRWLock.RUnlock()
		if pair.Tombstone {
			return "", false
		}

		return pair.Value, true
	}

	pair, err = lsmTree.secondaryTree.Find(key)

	if err == nil {

		lsmTree.treeRWLock.RUnlock()
		if pair.Tombstone {
			return "", false
		}
		return pair.Value, true
	}

	isPresent := lsmTree.BloomFilter.Contains(key)

	if !isPresent {
		return "", false
	}

	lsmTree.treeRWLock.RUnlock()
	lsmTree.diskRWLock.RLock()
	defer lsmTree.diskRWLock.RUnlock()

	for _, diskBlock := range lsmTree.diskFiles {
		pair, err = diskBlock.GetDataFromDisk(key)
		if err == nil {

			if pair.Tombstone {
				continue
			}
			return pair.Value, true
		}
	}

	return "", false
}

func (lsmTree *LSMTree) Put(key string, value string) {

	lsmTree.treeRWLock.Lock()
	defer lsmTree.treeRWLock.Unlock()

	Insert(&(lsmTree.tree), KV{key, value, false})

	go lsmTree.BloomFilter.Add(key)

	if lsmTree.tree.GetSize() >= lsmTree.flushThreshold && lsmTree.secondaryTree == nil {

		lsmTree.secondaryTree = lsmTree.tree
		lsmTree.tree = nil
		go lsmTree.Flush()
	}
}

func (lsmTree *LSMTree) Del(key string) {
	lsmTree.treeRWLock.Lock()
	defer lsmTree.treeRWLock.Unlock()

	Delete(&(lsmTree.tree), key)
	Delete(&(lsmTree.secondaryTree), key)

}

func (LSMTree *LSMTree) Flush() {
	newDiskBlocks := []DiskFile{NewDiskFile(LSMTree.secondaryTree.All())}

	LSMTree.diskRWLock.Lock()
	LSMTree.diskFiles = append(LSMTree.diskFiles, newDiskBlocks...)
	LSMTree.diskRWLock.Unlock()

	LSMTree.treeRWLock.Lock()
	LSMTree.secondaryTree = nil
	LSMTree.treeRWLock.Unlock()
}
