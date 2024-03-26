//go:build windows
// +build windows

package zfs

import (
	"syscall"
)

func procAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		HideWindow: true,
	}
}
