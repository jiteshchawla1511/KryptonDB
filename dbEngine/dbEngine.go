package dbengine

import (
	lsmtree "github.com/jiteshchawla1511/KryptonDB/LSM_Tree"
	"github.com/jiteshchawla1511/KryptonDB/wal"
)

type DBEngine struct {
	Lsmtree *lsmtree.LSMTree
	WAL     *wal.WAL
}
