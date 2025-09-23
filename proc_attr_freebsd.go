//go:build freebsd
// +build freebsd

package zfs

import (
	"syscall"
)

func procAttributes() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{
		// TODO:FIXME: Might require fix: https://github.com/golang/go/issues/27505#issuecomment-713706104
		Pdeathsig: syscall.SIGINT,
	}
}
