// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package reporting

import (
	"sync"
	"time"
)

type BenchmarkStats struct {
	TotalOps     uint64
	TotalGets    uint64
	TotalSets    uint64
	GetLatencies []time.Duration
	SetLatencies []time.Duration
	ErrorCount   uint64
	StartTime    time.Time
	LastReportAt time.Time
	StatLock     sync.Mutex
}
