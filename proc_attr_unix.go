//go:build !freebsd && !linux && !windows
// +build !freebsd,!linux,!windows

package zfs

import (
	"syscall"
)

func procAttributes() *syscall.SysProcAttr {
	return nil
}
