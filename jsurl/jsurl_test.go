package jsurl

import (
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		expected    interface{}
		encoded     string
		expectError bool
	}{
		{nil, "", false},
		{nil, "~null", false},
		{false, "~false", false},
		{true, "~true", false},
		{float64(0), "~0", false},
		{float64(1), "~1", false},
		{-1.5, "~-1.5", false},
		{"hello world\u203c", "~'hello*20world**203c", false},
		{" !\"#$%&'()*+,-./09:;<=>?@AZ[\\]^_`az{|}~", "~'*20*21*22*23!*25*26*27*28*29*2a*2b*2c-.*2f09*3a*3b*3c*3d*3e*3f*40AZ*5b*5c*5d*5e_*60az*7b*7c*7d*7e", false},
		{[]interface{}{}, "~(~)", false},
		{[]interface{}{nil, nil, nil, false, float64(0), "hello world\u203c"}, "~(~null~null~null~false~0~'hello*20world**203c)", false},
		{map[string]interface{}{}, "~()", false},
		{map[string]interface{}{
			"c": nil,
			"d": false,
			"e": float64(0),
			"f": "hello world\u203c",
		}, "~(c~null~d~false~e~0~f~'hello*20world**203c)", false},
		{map[string]interface{}{
			"a": []interface{}{
				[]interface{}{float64(1), float64(2)},
				[]interface{}{},
				map[string]interface{}{},
			},
			"b": []interface{}{},
			"c": map[string]interface{}{
				"d": "hello",
				"e": map[string]interface{}{},
				"f": []interface{}{},
			},
		}, "~(a~(~(~1~2)~(~)~())~b~(~)~c~(d~'hello~e~()~f~(~)))", false},
		{map[string]interface{}{
			"a": "hello",
			"b": "world",
		}, "~(a~%27hello~b~%27world)", false},
		{map[string]interface{}{
			"a": "hello",
			"b": "world",
		}, "~(a~%2527hello~b~%2525252527world)", false},
	}

	for _, tt := range tests {
		log.Printf("Testing %s", tt.encoded)
		parsed, err := Parse(tt.encoded)
		if tt.expectError {
			require.NotNil(t, err)
		} else {
			require.Nil(t, err)
		}

		require.Equal(t, tt.expected, parsed, "input was '%s'", tt.encoded)
	}
}
