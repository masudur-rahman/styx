package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAsBool_conversions(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want bool
	}{
		{"bool true", true, true},
		{"bool false", false, false},
		{"int64 one", int64(1), true},
		{"int64 zero", int64(0), false},
		{"int one", int(1), true},
		{"int zero", int(0), false},
		{"float nonzero", float64(1), true},
		{"float zero", float64(0), false},
		{"bytes one", []byte("1"), true},
		{"bytes zero", []byte("0"), false},
		{"bytes true", []byte("true"), true},
		{"bytes empty", []byte{}, false},
		{"bytes null byte", []byte{0x00}, false},
		{"string 1", "1", true},
		{"string true", "true", true},
		{"string TRUE", "TRUE", true},
		{"string True", "True", true},
		{"string t", "t", true},
		{"string 0", "0", false},
		{"string empty", "", false},
		{"unknown type", 3.14i, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, asBool(tt.in))
		})
	}
}
