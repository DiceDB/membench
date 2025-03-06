// Copyright (c) 2022-present, DiceDB contributors
// All rights reserved. Licensed under the BSD 3-Clause License. See LICENSE file in the project root for full license information.

package telemetry

type Sink interface {
	RecordLatencyOpInNanos(latency_ns float64, op string)
	RecordError(op string)
	PrintReport()
}
