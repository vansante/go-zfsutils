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
	args := []string{
		"zpool", "create", zpool,
	}

	for i := 0; i < 3; i++ {
		f, err := os.CreateTemp(os.TempDir(), "test-zpool-")
		noErr(err, fmt.Sprintf("create zpool file %d", i), "")
		err = f.Truncate(pow2(29))
		noErr(err, fmt.Sprintf("truncate zpool file %d", i), "")
		noErr(f.Close(), fmt.Sprintf("close zpool file %d", i), "")

		args = append(args, f.Name())

		defer os.Remove(f.Name()) // nolint:revive // its ok to defer to end of func
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
