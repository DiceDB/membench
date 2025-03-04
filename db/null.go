// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package db

import (
	"context"
)

type N struct {
}

func (r N) Get(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (r N) Set(ctx context.Context, key, value string) error {
	return nil
}

func (r N) Close() {
}

func NewNull() N {
	return N{}
}
