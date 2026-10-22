package parquet_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

type dateRow struct {
	Value   time.Time  `parquet:"value,date"`
	Pointer *time.Time `parquet:"pointer,optional,date"`
}

var dateTimes = []time.Time{
	time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
	time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC),
	// Not midnight: the day has to floor to Dec 31, not round toward 1970.
	time.Date(1969, 12, 31, 12, 0, 0, 0, time.UTC),
	time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
	time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
}

// writeRowsOneByOne writes through Writer.Write, which deconstructs each row
// instead of taking GenericWriter's batch path.
func writeRowsOneByOne(t *testing.T, rows []dateRow) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	w := parquet.NewWriter(buf, parquet.SchemaOf(new(dateRow)))
	for i := range rows {
		if err := w.Write(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestDateWritePaths(t *testing.T) {
	paths := []struct {
		name  string
		write func(*testing.T, []dateRow) []byte
	}{
		{"GenericWriter", writeRows[dateRow]},
		{"Writer", writeRowsOneByOne},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			rows := make([]dateRow, len(dateTimes))
			for i, ts := range dateTimes {
				ts := ts
				rows[i] = dateRow{Value: ts, Pointer: &ts}
			}
			data := path.write(t, rows)
			values, pointers := columnValues(t, data, "value"), columnValues(t, data, "pointer")
			got := readRows[dateRow](t, data, len(rows))
			for i, ts := range dateTimes {
				want := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
				wantDays := int32(want.Unix() / 86400)
				if v := values[i].Int32(); v != wantDays {
					t.Errorf("value %d: want day %d stored, got %d", i, wantDays, v)
				}
				if v := pointers[i].Int32(); v != wantDays {
					t.Errorf("pointer %d: want day %d stored, got %d", i, wantDays, v)
				}
				if !got[i].Value.Equal(want) {
					t.Errorf("value %d: want %v, got %v", i, want, got[i].Value)
				}
				if got[i].Pointer == nil || !got[i].Pointer.Equal(want) {
					t.Errorf("pointer %d: want %v, got %v", i, want, got[i].Pointer)
				}
			}
		})
	}
}
