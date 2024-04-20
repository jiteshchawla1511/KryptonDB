package diskstore

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	lsmtree "github.com/jiteshchawla1511/KryptonDB/LSM_Tree"
	"github.com/jiteshchawla1511/KryptonDB/wal"
)

var (
	ErrKeyNotFound = errors.New("key not found")
	ErrCAS         = errors.New("compare and swap issue")
)

const (
	DefaultNumOfPartitions = 10
	DefaultDirectory       = "/Users/jiteshchawla/KDB/KryptonDB/data"
)

type DiskStoreOpts struct {
	Directory       string
	NumOfPartitions int
}

type DiskStore struct {
	files []*os.File
	dir   string
	Locks []*sync.RWMutex
	Lock  sync.Mutex
}

func NewDisk(opts DiskStoreOpts) *DiskStore {

	dir := opts.Directory
	if !strings.HasSuffix(dir, "/") {
		dir = dir + "/"
	}

	numOfPartitions := opts.NumOfPartitions
	err := os.Mkdir(dir, 0755)
	if err != nil {
		return nil
	}

	disk := &DiskStore{
		dir:   dir,
		files: make([]*os.File, numOfPartitions),
		Locks: make([]*sync.RWMutex, numOfPartitions),
		Lock:  sync.Mutex{},
	}

	for i := 0; i < numOfPartitions; i++ {
		filename := fmt.Sprintf("%s/partition_%d", dir, i)
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {

			for j := 0; j < i; j++ {
				disk.files[j].Close()
				os.Remove(disk.files[j].Name())
			}
			return nil
		}
		disk.files[i] = file
		disk.Locks[i] = &sync.RWMutex{}
	}

	return disk
}

func partition(key string, numPartition int) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(numPartition))
}

func (disk *DiskStore) PersistToDisk(wl *wal.WAL, start <-chan bool) {
	<-start
	fmt.Println("starting the cycle")
	for {
		disk.Lock.Lock()
		var wg sync.WaitGroup
		entries := wl.ReadEntries()
		wg.Add(len(entries))

		for _, entry := range entries {
			go func(entry wal.Entry, wg *sync.WaitGroup) {
				partition := partition(entry.Key, len(disk.files))
				file := disk.files[partition]

				defer (*wg).Done()

				existingValue, err := disk.ReadValue(file, entry.Key, partition)
				if err != nil {
					fmt.Println(err)
					existingValue = nil
				}

				if entry.Delete {
					err = disk.DeleteFromDisk(file, entry.Key, partition)
					if err != nil {
						fmt.Printf("%s", err.Error())
					}
					return
				}

				err = disk.WriteValue(file, entry.Key, []byte(entry.Value), existingValue, partition)
				if err != nil {
					fmt.Printf("%s", err.Error())
				}
			}(entry, &wg)
		}

		wg.Wait()
		wl.Truncate()
		disk.Lock.Unlock()

		time.Sleep(5 * time.Second)
	}
}

func (disk *DiskStore) ReadValue(file *os.File, key string, partition int) ([]byte, error) {
	disk.Locks[partition].Lock()
	defer disk.Locks[partition].Unlock()

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if parts[0] == key {
			return []byte(parts[1]), nil
		}
	}
	return nil, nil
}

func (disk *DiskStore) WriteValue(file *os.File, key string, value []byte, existingValue []byte, parition int) error {
	disk.Locks[parition].RLock()
	defer disk.Locks[parition].RUnlock()

	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("%s:%s\n", key, value))

	if existingValue != nil {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.Split(line, ":")
			if parts[0] == key {
				continue
			}
			buf.WriteString(line + "\n")
		}
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())
	if err != nil {
		return err
	}
	return nil
}

func (disk *DiskStore) DeleteFromDisk(file *os.File, key string, partition int) error {
	disk.Locks[partition].RLock()
	defer disk.Locks[partition].RUnlock()

	_, err := file.Seek(0, io.SeekStart)

	if err != nil {
		return err
	}

	var buf bytes.Buffer
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		if parts[0] == key {
			continue
		}
		buf.WriteString(line + "\n")
	}

	if err := file.Truncate(0); err != nil {
		return err
	}

	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	_, err = file.Write(buf.Bytes())

	if err != nil {
		return err
	}

	return nil
}

func (disk *DiskStore) GetFileContents(i int) []wal.Entry {
	disk.Locks[i].RLock()
	defer disk.Locks[i].RUnlock()

	_, err := disk.files[i].Seek(0, io.SeekStart)
	if err != nil {
		return nil
	}

	scanner := bufio.NewScanner(disk.files[i])
	var entries []wal.Entry
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ":")
		entries = append(entries, wal.Entry{
			Key:   parts[0],
			Value: parts[1],
		})
	}

	return entries
}

func (disk *DiskStore) LoadFromDisk(lsmtree *lsmtree.LSMTree, wal *wal.WAL) error {
	for i := 0; i < len(disk.files); i++ {
		entry := disk.GetFileContents(i)

		for _, e := range entry {
			lsmtree.Put(e.Key, e.Value)
		}
	}

	err := wal.InitDB(lsmtree)

	if err != nil {
		return err
	}
	return nil
}
