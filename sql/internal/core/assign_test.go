package core

import (
	"testing"

	"github.com/google/uuid"
)

type uuidRow struct {
	ID   uuid.UUID `db:"id,pk"`
	Name string    `db:"name"`
}

type ptrRow struct {
	ID *uuid.UUID `db:"id,pk"`
}

type intRow struct {
	ID   int64 `db:"id,pk autoincr"`
	Name string
}

// A uuid column comes back from lib/pq as the 36 text characters. Go converts
// []byte to [16]byte by truncation, so without the Scanner path the id is
// silently replaced by the ASCII of its own first half.
func TestAssignIDParsesDriverTextIntoUUID(t *testing.T) {
	want := uuid.MustParse("018f3a10-0000-7000-8000-0000000000b1")

	for _, tc := range []struct {
		name string
		id   any
	}{
		{"bytes", []byte(want.String())},
		{"string", want.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			row := &uuidRow{Name: "x"}
			if err := AssignID(row, tc.id); err != nil {
				t.Fatalf("AssignID: %v", err)
			}
			if row.ID != want {
				t.Fatalf("ID = %s, want %s", row.ID, want)
			}
		})
	}
}

func TestAssignIDAllocatesPointerUUID(t *testing.T) {
	want := uuid.MustParse("018f3a10-0000-7000-8000-0000000000b2")
	row := &ptrRow{}
	if err := AssignID(row, []byte(want.String())); err != nil {
		t.Fatalf("AssignID: %v", err)
	}
	if row.ID == nil || *row.ID != want {
		t.Fatalf("ID = %v, want %s", row.ID, want)
	}
}

func TestAssignIDConvertsSerialID(t *testing.T) {
	row := &intRow{Name: "x"}
	if err := AssignID(row, int64(42)); err != nil {
		t.Fatalf("AssignID: %v", err)
	}
	if row.ID != 42 {
		t.Fatalf("ID = %d, want 42", row.ID)
	}
}

// A conversion that would truncate is an error, not a silently wrong id.
func TestAssignIDRejectsTruncatingConversion(t *testing.T) {
	type arrayRow struct {
		ID [4]byte `db:"id,pk"`
	}
	row := &arrayRow{}
	if err := AssignID(row, []byte("0123456789")); err == nil {
		t.Fatalf("expected an error, got ID = %v", row.ID)
	}
}
