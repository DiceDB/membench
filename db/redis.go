// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type R struct {
	c *redis.Client
}

func (r R) Get(ctx context.Context, key string) (string, error) {
	res, err := r.c.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return res, err
}

func (r R) Set(ctx context.Context, key, value string) error {
	return r.c.Set(ctx, key, value, 0).Err()
}

func (r R) Close() {
	r.c.Close()
}

func NewRedis(host string, port int) R {
	c := redis.NewClient(&redis.Options{
		Addr: fmt.Sprintf("%s:%d", host, port),
	})
	return R{c: c}
}
