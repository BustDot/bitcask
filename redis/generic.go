package redis

import "errors"

func (rds *RedisDataStructure) Del(key []byte) error {
	return rds.db.Delete(key)
}

func (rds *RedisDataStructure) Type(key []byte) (RedisDataType, error) {
	value, err := rds.db.Get(key)
	if err != nil {
		return 0, err
	}
	if len(value) == 0 {
		return 0, errors.New("value is null")
	}
	return value[0], nil
}
