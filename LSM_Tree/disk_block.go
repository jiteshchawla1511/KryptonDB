package lsmtree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"strconv"
)

const (
	maxFileLen       = 1024
	indexSparseRatio = 10
)

type DiskFile struct {
	index            *Node
	buffer           bytes.Buffer
	NumberOfElements int
}

func (d DiskFile) Empty() bool {
	return d.NumberOfElements == 0
}

func NewDiskFile(elements []KV) DiskFile {
	diskFile := DiskFile{NumberOfElements: len(elements)}
	IndexElem := make([]KV, 0)
	var encoder *gob.Encoder

	for i, element := range elements {
		if i%indexSparseRatio == 0 {
			idx := KV{Key: element.Key, Value: fmt.Sprintf("%d", diskFile.buffer.Len())}
			log.Printf("created sparse index element %v", idx)
			IndexElem = append(IndexElem, idx)
			encoder = gob.NewEncoder(&diskFile.buffer)
		}
		encoder.Encode(element)
	}
	diskFile.index = NewTree(IndexElem)
	return diskFile
}
func (d *DiskFile) GetDataFromDisk(key string) (KV, error) {
	if d.Empty() {
		return KV{}, fmt.Errorf("disk is empty")
	}

	// technically we are findind a search space of [l.......r] and in this block, we will decode and
	// check whether our key is this range or not
	// to find this range we used LOGN complexity as we are using binary tree
	start, err := d.index.JustSmallerOrEqual(key)
	if err != nil {
		return KV{}, err
	}

	end, err := d.index.JustGreater(key)
	if err != nil {
		return KV{}, err
	}

	// Convert start and end to int
	LeftIndex, _ := strconv.Atoi(start.Value)
	RightIndex, _ := strconv.Atoi(end.Value)

	// Create an iterator for the search space
	iterator := d.buffer.Bytes()[LeftIndex:RightIndex]

	// Decode and search for the key
	for {
		curr, err := decodeNextKV(iterator)
		if err == io.EOF {
			break
		} else if err != nil {
			return KV{}, err
		}

		if curr.Key == key {
			return curr, nil
		}
	}

	return KV{}, fmt.Errorf("key not found ")
}

func decodeNextKV(buffer []byte) (KV, error) {
	curr := KV{}
	decoder := gob.NewDecoder(bytes.NewReader(buffer))
	err := decoder.Decode(&curr)

	return curr, err
}

func (d DiskFile) Search(key string) (KV, error) {
	canErr := fmt.Errorf("key %s not found in disk file", key)
	if d.Empty() {
		return KV{}, canErr
	}
	var si, ei int
	start, err := d.index.JustSmallerOrEqual(key)
	if err != nil {
		// Key smaller than all.
		return KV{}, canErr
	}
	si, _ = strconv.Atoi(start.Value)
	end, err := d.index.JustGreater(key)
	if err != nil {
		// Key larger than all or equal to the last one.
		ei = d.buffer.Len()
	} else {
		ei, _ = strconv.Atoi(end.Value)
	}
	log.Printf("searching in range [%d,%d)]", si, ei)
	buf := bytes.NewBuffer(d.buffer.Bytes()[si:ei])
	dec := gob.NewDecoder(buf)
	for {
		var e KV
		if err := dec.Decode(&e); err != nil {
			log.Printf("got err: %v", err)
			break
		}
		if e.Key == key {
			return e, nil
		}
	}
	return KV{}, canErr
}

func (d *DiskFile) Delete(key string) error {

	if d.Empty() {
		return fmt.Errorf("disk is empty")
	}

	start, err := d.index.JustSmallerOrEqual(key)
	if err != nil {
		return err
	}

	end, err := d.index.JustGreater(key)
	if err != nil {
		return err
	}

	LeftIndex, _ := strconv.Atoi(start.Value)
	RightIndex, _ := strconv.Atoi(end.Value)

	searchBuffer := bytes.NewBuffer(d.buffer.Bytes()[LeftIndex:RightIndex])

	DecodedSearchBuffer := gob.NewDecoder(searchBuffer)

	var elemnts []KV

	for {
		var curr KV
		err := DecodedSearchBuffer.Decode(&curr)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if curr.Key != key {
			elemnts = append(elemnts, curr)
		}
	}
	d.buffer.Reset()
	for _, curr := range elemnts {
		encoder := gob.NewEncoder(&d.buffer)
		encoder.Encode(curr)
	}
	d.NumberOfElements--
	return nil
}

func (d *DiskFile) All() []KV {
	elements := d.index.All()
	var list []KV

	for i, ele := range elements {
		StartIndex, _ := strconv.Atoi(ele.Value)
		var EndIndex int
		if i < len(elements)-1 {
			EndIndex, _ = strconv.Atoi(elements[i+1].Value)
		} else {
			EndIndex = d.buffer.Len()
		}
		searchBuffer := bytes.NewBuffer(d.buffer.Bytes()[StartIndex:EndIndex])

		DecodedSearchBuffer := gob.NewDecoder(searchBuffer)
		var curr KV

		for DecodedSearchBuffer.Decode(&curr) == nil {
			list = append(list, curr)
		}
	}
	return list

}
