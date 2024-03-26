//go:build freebsd
// +build freebsd

package zfs

import (
	"syscall"
)

func procAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGINT,
	}
}
