/*
    _____           _____   _____   ____          ______  _____  ------
   |     |  |      |     | |     | |     |     | |       |            |
   |     |  |      |     | |     | |     |     | |       |            |
   | --- |  |      |     | |-----| |----  _____| |-----| |-----  ------
   |     |  |      |     | |     | |     |     |       | |       |
   | ____|  |_____ | ____| | ____| |     |_____|  _____| |_____  |_____


   Licensed under the MIT License <http://opensource.org/licenses/MIT>.

   Copyright © 2020-2026 Microsoft Corporation. All rights reserved.
*/

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMonitorPidPatternBoundaries(t *testing.T) {
	p := monitorPidPattern("12")
	line := func(args string) string { return "  4242 /usr/bin/bfusemon " + args + " --config-file=x" }

	assert.True(t, p.MatchString(line("--pid=12")))
	assert.True(t, p.MatchString(line("--pid=12 --cache-path=/c")))
	assert.True(t, p.MatchString(line("--output-path=/x --pid=12")))

	// Prefix collision: --pid=123 must not match target 12.
	assert.False(t, p.MatchString(line("--pid=123")))
	assert.False(t, p.MatchString(line("--pid=121")))
	assert.False(t, p.MatchString(line("--pid=112")))

	// Numeric uid/path that merely contains the digits must not match.
	assert.False(t, p.MatchString("  7 /usr/bin/bfusemon --cache-path=/tmp/12/files"))
	assert.False(t, p.MatchString("  8 /usr/bin/bfusemon --output-path=/var/run/12"))
}

func TestGetMonitorPidsParsing(t *testing.T) {
	// Exercise the regex/parse logic directly without invoking ps.
	p := monitorPidPattern("100")
	cases := map[string]bool{
		"9001 /usr/bin/bfusemon --pid=100":                    true,
		"9002 /usr/bin/bfusemon --pid=1000":                   false,
		"9003 /usr/bin/bfusemon --pid=99 --pid=100":           true,
		"9004 /usr/bin/blobfuse2 mount /mnt --pid-file=x":    false,
	}
	for line, want := range cases {
		got := p.MatchString(" " + line)
		assert.Equal(t, want, got, line)
	}
}
