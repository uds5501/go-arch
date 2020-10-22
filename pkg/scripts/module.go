package scripts

import (
	"sync"
	"trell/go-arch/db"
)

type Module struct {
	script *Operations
}

var moduleSingleton *Module
var moduleSingletonOnce sync.Once

func NewScriptsModuleSingleton(db db.DBFactory) *Module {
	moduleSingletonOnce.Do(func() {
		script := NewOperation(db)
		moduleSingleton = &Module{
			script: script,
		}
	})

	return moduleSingleton
}

func (m *Module) GetScript() *Operations {
	return m.script
}
