package zfs

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPropertySources_StringSlice(t *testing.T) {
	tests := []struct {
		name string
		in   PropertySources
		want []string
	}{
		{
			"empty",
			PropertySources{},
			[]string{},
		},
		{
			"single",
			PropertySources{PropertySourceLocal},
			[]string{"local"},
		},
		{
			"multiple",
			PropertySources{PropertySourceLocal, PropertySourceInherited, PropertySourceReceived},
			[]string{"local", "inherited", "received"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.in.StringSlice())
		})
	}
}
