package data

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"time"
)

var (
	client = &LogRedisClient{}
)

type LogRedisClient struct {
	c *redis.Client
}

//GetClient get the redis client
func InitRedis() *LogRedisClient {
	c := redis.NewClient(&redis.Options{
		Addr:     "redis-12817.c275.us-east-1-4.ec2.cloud.redislabs.com:12817", // host:port of the redis server
		Password: "sddsddsdd", // no password set
		DB:       0,  // use default DB
	})

	if err := c.Ping().Err(); err != nil {
		panic("Unable to connect to redis " + err.Error())
	}
	client.c = c
	return client
}

//GetKey get key
func (client *LogRedisClient) GetKey(key string, src interface{}) error {
	val, err := client.c.Get(key).Result()
	if err == redis.Nil || err != nil {
		return err
	}
	err = json.Unmarshal([]byte(val), &src)
	if err != nil {
		return err
	}
	return nil
}

//SetKey set key
func (client *LogRedisClient) SetKey(key string, value interface{}, expiration time.Duration) error {
	cacheEntry, err := json.Marshal(value)
	if err != nil {
		return err
	}
	err = client.c.Set(key, cacheEntry, expiration).Err()
	if err != nil {
		return err
	}
	return nil
}
