// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package db

type N struct {
}

func (*N) Get(key string) (string, error) {
	return "", nil
}

func (*N) Set(key, value string) error {
	return nil
}
