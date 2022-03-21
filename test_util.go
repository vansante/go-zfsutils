package zfs

import (
	"context"
	"fmt"
	"io/ioutil"
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
	noErr := func(err error, out string) {
		if err != nil {
			fmt.Println(out)
			panic(err)
		}
	}
	args := []string{
		"zpool", "create", zpool,
	}

	for i := 0; i < 3; i++ {
		f, err := ioutil.TempFile(os.TempDir(), "zfs-zpool-")
		noErr(err, "")
		err = f.Truncate(pow2(29))
		noErr(err, "")
		noErr(f.Close(), "")

		args = append(args, f.Name())

		defer os.Remove(f.Name()) // nolint:revive // its ok to defer to end of func
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sudo", args...)
	out, err := cmd.CombinedOutput()
	noErr(err, string(out))

	cmd = exec.CommandContext(ctx, "sudo",
		"zfs", "allow", "everyone",
		strings.Join(zfsPermissions, ","),
		zpool,
	)
	out, err = cmd.CombinedOutput()
	noErr(err, string(out))

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "sudo", "zpool", "destroy", zpool)
		out, err := cmd.CombinedOutput()
		noErr(err, string(out))
	}()

	fn()
}

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}
