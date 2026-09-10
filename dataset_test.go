package zfs

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_readDatasets(t *testing.T) {
	in := splitOutput(testInput)

	const prop1 = "nl.test:hiephoi"
	const prop2 = "nl.test:eigenschap"

	ds, err := readDatasets(in, []string{prop1, prop2})
	require.NoError(t, err)
	require.Len(t, ds, 3)
	require.Equal(t, ds[0].Name, "testpool/ds0")
	require.Equal(t, ds[1].Name, "testpool/ds1")
	require.Equal(t, ds[2].Name, "testpool/ds10")

	for i := range ds {
		require.Equal(t, "", ds[i].Origin)
		require.NotEmpty(t, ds[i].Name)
		require.NotEmpty(t, ds[i].Mountpoint)
		require.True(t, ds[i].Mounted || i == 2)
		require.NotZero(t, ds[i].Referenced)
		require.NotZero(t, ds[i].Used)
		require.NotZero(t, ds[i].Available)
		require.Equal(t, "42", ds[i].ExtraProps[prop1])
		require.Equal(t, "ja", ds[i].ExtraProps[prop2])
	}
}

func Test_readDatasets_invalidLineCount(t *testing.T) {
	in := splitOutput(testInput)

	// Drop a line so the total is no longer a multiple of the property count
	_, err := readDatasets(in[:len(in)-1], []string{"nl.test:hiephoi", "nl.test:eigenschap"})
	require.ErrorContains(t, err, "output invalid")
}

func Test_readDatasets_invalidFieldCount(t *testing.T) {
	// Keep exactly one dataset worth of lines, so the multiple check passes
	lines := strings.Split(testInput, "\n")[:len(dsPropList)]
	lines[2] = "testpool/ds0\torigin"

	_, err := readDatasets(splitOutput(strings.Join(lines, "\n")+"\n"), nil)
	require.ErrorContains(t, err, "output contains line with 2 fields")
}

func Test_readDatasets_invalidUint(t *testing.T) {
	in := splitOutput(strings.Replace(testInput,
		"testpool/ds1\tused\t196416",
		"testpool/ds1\tused\tnotanumber",
		1,
	))

	_, err := readDatasets(in, []string{"nl.test:hiephoi", "nl.test:eigenschap"})
	require.ErrorContains(t, err, "error in dataset 1 (testpool/ds1) field used [notanumber]")
	require.ErrorIs(t, err, strconv.ErrSyntax)
}

const testInput = `testpool/ds0	name	testpool/ds0
testpool/ds0	type	filesystem
testpool/ds0	origin	-
testpool/ds0	used	196416
testpool/ds0	available	186368146928528
testpool/ds0	mounted	yes
testpool/ds0	mountpoint	none
testpool/ds0	compression	off
testpool/ds0	volsize	-
testpool/ds0	quota	0
testpool/ds0	refquota	0
testpool/ds0	referenced	196416
testpool/ds0	written	196416
testpool/ds0	logicalused	43520
testpool/ds0	usedbydataset	196416
testpool/ds0	nl.test:hiephoi	42
testpool/ds0	nl.test:eigenschap	ja
testpool/ds1	name	testpool/ds1
testpool/ds1	type	filesystem
testpool/ds1	origin	-
testpool/ds1	used	196416
testpool/ds1	available	186368146928528
testpool/ds1	mounted	yes
testpool/ds1	mountpoint	none
testpool/ds1	compression	off
testpool/ds1	volsize	-
testpool/ds1	quota	0
testpool/ds1	refquota	0
testpool/ds1	referenced	196416
testpool/ds1	written	196416
testpool/ds1	logicalused	43520
testpool/ds1	usedbydataset	196416
testpool/ds1	nl.test:hiephoi	42
testpool/ds1	nl.test:eigenschap	ja
testpool/ds10	name	testpool/ds10
testpool/ds10	type	filesystem
testpool/ds10	origin	-
testpool/ds10	used	196416
testpool/ds10	available	186368146928528
testpool/ds10	mounted	no
testpool/ds10	mountpoint	none
testpool/ds10	compression	off
testpool/ds10	volsize	-
testpool/ds10	quota	0
testpool/ds10	refquota	0
testpool/ds10	referenced	196416
testpool/ds10	written	196416
testpool/ds10	logicalused	43520
testpool/ds10	usedbydataset	196416
testpool/ds10	nl.test:hiephoi	42
testpool/ds10	nl.test:eigenschap	ja
`
