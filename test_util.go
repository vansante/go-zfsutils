package zfs

import (
	"context"
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
	noErr := func(err error) {
		if err != nil {
			panic(err)
		}
	}
	args := []string{
		"zpool", "create", zpool,
	}

	for i := 0; i < 3; i++ {
		f, err := ioutil.TempFile(os.TempDir(), "zfs-zpool-")
		noErr(err)
		err = f.Truncate(pow2(29))
		noErr(f.Close())
		noErr(err)

		args = append(args, f.Name())

		defer os.Remove(f.Name()) // nolint:revive // its ok to defer to end of func
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "sudo", args...)
	_, err := cmd.CombinedOutput()
	noErr(err)

	cmd = exec.CommandContext(ctx, "sudo",
		"zfs", "allow", "everyone",
		strings.Join(zfsPermissions, ","),
		zpool,
	)
	_, err = cmd.CombinedOutput()
	noErr(err)

	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "sudo", "zpool", "destroy", zpool)
		_, err := cmd.Output()
		noErr(err)
	}()

	fn()
}

func sleep(delay int) {
	time.Sleep(time.Duration(delay) * time.Second)
}

func pow2(x int) int64 {
	return int64(math.Pow(2, float64(x)))
}
