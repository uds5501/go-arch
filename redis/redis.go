package redis

import (
	"sync"

	"trell/go-arch/config"

	"github.com/go-redis/redis"
	"go.elastic.co/apm/module/apmgoredis"
)

var client apmgoredis.Client
var once sync.Once

func Init() {
	once.Do(func() {
		client = apmgoredis.Wrap(redis.NewClient(&redis.Options{
			Addr: config.Get().RedisAddr,
		}))
		_, err := client.Ping().Result()
		if err != nil {
			panic(err.Error())
		}
	})
}

func Client() apmgoredis.Client {
	return client
}
