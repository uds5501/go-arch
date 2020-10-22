package common

import (
	"sync"
	"trell/go-arch/db"
)

type Module struct {
	util            *Util

}

var moduleSingleton *Module
var moduleSingletonOnce sync.Once

func NewModuleSingleton(db db.DBFactory) *Module {
	moduleSingletonOnce.Do(func() {
		util := NewUtil(db)
		//matrices := NewMatrices()
		moduleSingleton = &Module{
			util: util,
			//matrices: matrices,
		}
	})

	return moduleSingleton
}

func (m *Module) GetUtil() *Util {
	return m.util
}


