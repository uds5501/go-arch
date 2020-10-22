package server

import (
	"trell/go-arch/db"
	"trell/go-arch/logger"
	"trell/go-arch/pkg/scripts"
)

func Init() {
	logger.Init()
	db.Init()
	//es.Init()
	//redis.Init()
	scriptsModule := scripts.NewScriptsModuleSingleton(db.Factory)
	scriptsModule.GetScript().Init()
	// r := NewRouter()
	// r.Run(":" + "4000")

}
