package main

import (
	"bitcask"
	bitcaskredis "bitcask/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcaskredis.RedisDataStructure
	server *redcon.Server
	mu     *sync.RWMutex
}

func main() {
	redisDataStructure, err := bitcaskredis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcaskredis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	// Initialize the Redis server
	bitcaskServer.server = redcon.NewServer(addr, nil, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server is running on", addr)
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
