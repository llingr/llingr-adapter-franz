// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-franz Authors
// SPDX-License-Identifier: Apache-2.0

package franzadapter

import "testing"

// closeOnPanic must call closeFn and step over any subsequent panics.
// This means closing a client while a primary panic propagates through
// CreateConsumer will not mask that primary panic.
func TestCloseOnPanicSwallowsSecondaryPanic(t *testing.T) {
	closed := false
	a := &Adapter{closeFn: func() { closed = true; panic("close boom") }}
	a.closeOnPanic() // must return normally despite closeFn panicking
	if !closed {
		t.Fatal("closeOnPanic did not call closeFn")
	}
}

// A nil closeFn (e.g. a NewCustom adapter before SetClient) must be a safe no-op.
func TestCloseOnPanicNilCloseFn(t *testing.T) {
	a := &Adapter{}
	a.closeOnPanic() // must not panic
}
