//go:build amd64 && linux

package mbbolt

import "syscall"

const DefaultMMapFlags = syscall.MAP_POPULATE
