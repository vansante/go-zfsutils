package zfs

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"
)

// sudo zfs allow <user> canmount,clone,compression,create,destroy,encryption,keyformat,keylocation,load-key,mount,
// mountpoint,promote,readonly,receive,refquota,refreservation,rename,rollback,send,snapshot,userprop,volblocksize,
// volmode,volsize <dataset>
var zfsPermissions = []string{
	"canmount",
	"clone",
	"compression",
	"create",
	"destroy",
	"encryption",
	"keyformat",
	"keylocation",
	"load-key",
	"mount",
	"mountpoint",
	"promote",
	"readonly",
	"receive",
	"refquota",
	"refreservation",
	"rename",
	"rollback",
	"send",
	"snapshot",
	"userprop",
	"volblocksize",
	"volmode",
	"volsize",
}

// TestZPool uses some temp files to create a zpool with the given name to run tests with
func TestZPool(zpool string, fn func()) {
	noErr := func(err error, context, out string) {
		if err != nil {
			fmt.Println("context: " + context)
			fmt.Println("output: " + out)
			panic(err)
		}
	}

	zFile, err := os.CreateTemp(os.TempDir(), "test-zpool-")
	noErr(err, "create zpool file", "")
	err = zFile.Truncate(pow2(28))
	noErr(err, "truncate zpool file", "")
	noErr(zFile.Close(), "close zpool file", "")
	defer os.Remove(zFile.Name())

	args := []string{
		"zpool", "create", zpool, zFile.Name(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sudo", args...)
	out, err := cmd.CombinedOutput()
	noErr(err, "sudo "+strings.Join(args, " "), string(out))

	cmd = exec.CommandContext(ctx, "sudo",
		"zfs", "allow", "everyone",
		strings.Join(zfsPermissions, ","),
		zpool,
	)
	out, err = cmd.CombinedOutput()
	noErr(err, "sudo zfs allow everyone "+strings.Join(zfsPermissions, ",")+" "+zpool, string(out))

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "sudo", "zpool", "destroy", zpool)
		out, err := cmd.CombinedOutput()
		noErr(err, "sudo zpool destroy "+zpool, string(out))
	}()

	fn()
}

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}
