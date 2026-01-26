// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTopRUEnableDisableAndResetInterval(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUReportInterval()

	require.False(t, TopRUEnabled())
	EnableTopRU()
	require.True(t, TopRUEnabled())
	EnableTopRU()
	require.True(t, TopRUEnabled())

	SetTopRUReportInterval(15)
	require.Equal(t, int64(15), GetTopRUReportInterval())

	DisableTopRU()
	require.True(t, TopRUEnabled())
	require.Equal(t, int64(15), GetTopRUReportInterval())

	DisableTopRU()
	require.False(t, TopRUEnabled())
	require.Equal(t, int64(DefTiDBTopRUReportIntervalSeconds), GetTopRUReportInterval())

	// Defensive extra disable should not underflow.
	DisableTopRU()
	require.False(t, TopRUEnabled())
}

func TestTopRUReportIntervalSmallerPrevails(t *testing.T) {
	GlobalState.ruConsumerCount.Store(0)
	ResetTopRUReportInterval()

	SetTopRUReportInterval(30)
	require.Equal(t, int64(30), GetTopRUReportInterval())

	// Larger interval should not overwrite smaller one.
	SetTopRUReportInterval(60)
	require.Equal(t, int64(30), GetTopRUReportInterval())

	SetTopRUReportInterval(15)
	require.Equal(t, int64(15), GetTopRUReportInterval())
}
