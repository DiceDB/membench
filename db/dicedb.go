// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package db

import (
	"context"
	"errors"

	"github.com/dicedb/dicedb-go"
	"github.com/dicedb/dicedb-go/wire"
)

type D struct {
	c *dicedb.Client
}

func (r D) Get(ctx context.Context, key string) (string, error) {
	res := r.c.Fire(&wire.Command{
		Cmd:  "GET",
		Args: []string{key},
	})

	if res.Err != "" {
		return "", errors.New(res.Err)
	}
	return res.GetVStr(), nil
}

func (r D) Set(ctx context.Context, key, value string) error {
	res := r.c.Fire(&wire.Command{
		Cmd:  "SET",
		Args: []string{key, value},
	})

	if res.Err != "" {
		return errors.New(res.Err)
	}
	return nil
}

func (r D) Close() {
	r.c.Close()
}

func NewDiceDB(host string, port int) D {
	c, err := dicedb.NewClient(host, port)
	if err != nil {
		panic(err)
	}
	return D{c: c}
}
