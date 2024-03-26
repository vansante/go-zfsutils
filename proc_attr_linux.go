//go:build linux
// +build linux

package zfs

import (
	"syscall"
)

func procAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGINT,
	}
}
