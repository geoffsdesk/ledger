package backend

import "testing"

func TestPrefixEnd(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/registry/pods/", "/registry/pods0"}, // '/' (0x2f) -> '0' (0x30)
		{"ab", "ac"},
		{"a\xff", "b"}, // trailing 0xff is dropped, previous byte incremented
		{"", ""},       // empty prefix -> unbounded
		{"\xff\xff", ""}, // all 0xff -> unbounded
	}
	for _, c := range cases {
		if got := PrefixEnd(c.in); got != c.want {
			t.Errorf("PrefixEnd(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
