package main

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func TestParseFieldFilters(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    []fieldFilter
		wantErr bool
	}{
		{"empty", "", nil, false},
		{"single eq", "id=1", []fieldFilter{{"id", "1"}}, false},
		{"single colon", "id:1", []fieldFilter{{"id", "1"}}, false},
		{"multi", "id=1,name=foo", []fieldFilter{{"id", "1"}, {"name", "foo"}}, false},
		{"outer whitespace", " id=1 , name=foo ", []fieldFilter{{"id", "1"}, {"name", "foo"}}, false},
		{"missing sep", "abc", nil, true},
		{"empty name", "=v", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFieldFilters(tt.raw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if len(got) != len(tt.want) {
				t.Fatalf("len(got) = %d, want %d (%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("[%d] got %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestRowValueString(t *testing.T) {
	if got := rowValueString([]byte("abc")); got != "abc" {
		t.Errorf("bytes: got %q", got)
	}
	if got := rowValueString(int64(42)); got != "42" {
		t.Errorf("int: got %q", got)
	}
	if got := rowValueString(nil); got != "<nil>" {
		t.Errorf("nil: got %q", got)
	}
}

const (
	uuidA = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	uuidB = "4a3e1234-71ca-11e1-9e33-c80aa9429562"
)

func TestGetUUIDSet(t *testing.T) {
	set := uuidA + ":1-10," + uuidB + ":1-5"
	if u := getUUIDSet(uuidA, set); u == nil || u.SID.String() != uuidA {
		t.Fatalf("uuidA lookup failed: %v", u)
	}
	if u := getUUIDSet("00000000-0000-0000-0000-000000000000", set); u != nil {
		t.Fatalf("unexpected match: %v", u)
	}
	if u := getUUIDSet(uuidA, ""); u != nil {
		t.Fatalf("empty gtid should return nil, got %v", u)
	}
}

func TestTryGetLastUUID(t *testing.T) {
	set := uuidA + ":1-10," + uuidB + ":1-5"
	if got := tryGetLastUUID(uuidA, set); got != uuidB {
		t.Errorf("tryGetLastUUID = %q, want %q", got, uuidB)
	}
	if got := tryGetLastUUID(uuidA, ""); got != "" {
		t.Errorf("empty set: got %q", got)
	}
	if got := tryGetLastUUID(uuidA, uuidA+":1-10"); got != "" {
		t.Errorf("only self: got %q, want empty", got)
	}
}

func TestReplaceGtidByUUID(t *testing.T) {
	set := uuidA + ":1-10," + uuidB + ":1-5"
	got := replaceGtidByUUID(uuidA, uuidA+":1-20", set)
	// order is preserved: entries other than replaced uuid, then the new one
	want := uuidB + ":1-5," + uuidA + ":1-20"
	if got != want {
		t.Errorf("replaceGtidByUUID = %q, want %q", got, want)
	}
}

func TestShrinkLastInterval(t *testing.T) {
	// 1-10 => shrink to 1-9
	s, _ := mysql.ParseUUIDSet(uuidA + ":1-10")
	got := shrinkLastInterval(s)
	if got == nil || got.String() != uuidA+":1-9" {
		t.Fatalf("shrink 1-10: got %v", got)
	}

	// 5 (single element) => should drop the interval entirely and return nil
	s, _ = mysql.ParseUUIDSet(uuidA + ":5")
	if got := shrinkLastInterval(s); got != nil {
		t.Fatalf("shrink single: got %v, want nil", got)
	}

	// 1-3:10 => shrink 10 to nothing, keep 1-3
	s, _ = mysql.ParseUUIDSet(uuidA + ":1-3:10")
	got = shrinkLastInterval(s)
	if got == nil || got.String() != uuidA+":1-3" {
		t.Fatalf("shrink 1-3:10: got %v", got)
	}

	// nil / empty
	if got := shrinkLastInterval(nil); got != nil {
		t.Fatalf("nil input: got %v", got)
	}
}

func TestConfigServerIDDefaultRandom(t *testing.T) {
	c := &Config{}
	id1 := c.serverID()
	if id1 == 0 {
		t.Fatal("serverID must not be zero")
	}
	// Explicit ServerID wins.
	c.ServerID = 42
	if got := c.serverID(); got != 42 {
		t.Fatalf("serverID = %d, want 42", got)
	}
}
