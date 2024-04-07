package dbengine

import (
	lsmtree "github.com/jiteshchawla1511/KryptonDB/LSM_Tree"
	diskstore "github.com/jiteshchawla1511/KryptonDB/diskStore"
	"github.com/jiteshchawla1511/KryptonDB/wal"
)

type DBEngine struct {
	Lsmtree *lsmtree.LSMTree
	WAL     *wal.WAL
	Store   *diskstore.DiskStore
}

func (db *DBEngine) LoadFromDisk(lsmTree *lsmtree.LSMTree, wal *wal.WAL) error {
	return db.Store.LoadFromDisk(lsmTree, wal)
}
