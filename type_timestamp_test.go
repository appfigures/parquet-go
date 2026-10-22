package parquet_test

import (
	"bytes"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// Millisecond and microsecond timestamps cover far more than the ±292 years an
// int64 count of nanoseconds can hold, so none of these may be read back by way
// of a nanosecond value.
var outOfNanosRangeTimes = []time.Time{
	time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
	time.Date(1600, 1, 1, 12, 34, 56, 789012000, time.UTC),
	time.Date(1677, 9, 21, 0, 12, 43, 145224000, time.UTC),
	time.Date(1969, 12, 31, 23, 59, 59, 999999000, time.UTC),
	time.Date(2262, 4, 11, 23, 47, 16, 854776000, time.UTC),
	time.Date(2500, 6, 1, 8, 0, 0, 1000, time.UTC),
	time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
}

func TestTimestampOutOfNanosRangeRoundTrip(t *testing.T) {
	type millisRow struct {
		Value   time.Time  `parquet:"value,timestamp(millisecond)"`
		Pointer *time.Time `parquet:"pointer,optional,timestamp(millisecond)"`
	}
	type microsRow struct {
		Value   time.Time  `parquet:"value,timestamp(microsecond)"`
		Pointer *time.Time `parquet:"pointer,optional,timestamp(microsecond)"`
	}

	t.Run("millisecond", func(t *testing.T) {
		rows := make([]millisRow, len(outOfNanosRangeTimes))
		for i, ts := range outOfNanosRangeTimes {
			ts := ts
			rows[i] = millisRow{Value: ts, Pointer: &ts}
		}
		got := roundTripRows(t, rows)
		for i, ts := range outOfNanosRangeTimes {
			want := ts.Truncate(time.Millisecond)
			if !got[i].Value.Equal(want) {
				t.Errorf("value %d: want %v, got %v", i, want, got[i].Value)
			}
			if got[i].Pointer == nil || !got[i].Pointer.Equal(want) {
				t.Errorf("pointer %d: want %v, got %v", i, want, got[i].Pointer)
			}
		}
	})

	t.Run("microsecond", func(t *testing.T) {
		rows := make([]microsRow, len(outOfNanosRangeTimes))
		for i, ts := range outOfNanosRangeTimes {
			ts := ts
			rows[i] = microsRow{Value: ts, Pointer: &ts}
		}
		got := roundTripRows(t, rows)
		for i, ts := range outOfNanosRangeTimes {
			want := ts.Truncate(time.Microsecond)
			if !got[i].Value.Equal(want) {
				t.Errorf("value %d: want %v, got %v", i, want, got[i].Value)
			}
			if got[i].Pointer == nil || !got[i].Pointer.Equal(want) {
				t.Errorf("pointer %d: want %v, got %v", i, want, got[i].Pointer)
			}
		}
	})
}

func TestDateOutOfNanosRangeRoundTrip(t *testing.T) {
	type row struct {
		Value time.Time `parquet:"value,date"`
	}
	rows := make([]row, len(outOfNanosRangeTimes))
	for i, ts := range outOfNanosRangeTimes {
		rows[i] = row{Value: ts}
	}
	got := roundTripRows(t, rows)
	for i, ts := range outOfNanosRangeTimes {
		want := time.Date(ts.Year(), ts.Month(), ts.Day(), 0, 0, 0, 0, time.UTC)
		if !got[i].Value.Equal(want) {
			t.Errorf("value %d: want %v, got %v", i, want, got[i].Value)
		}
	}
}

func roundTripRows[T any](t *testing.T, rows []T) []T {
	t.Helper()
	return readRows[T](t, writeRows(t, rows), len(rows))
}

// writeRows writes through GenericWriter, the batch path.
func writeRows[T any](t *testing.T, rows []T) []byte {
	t.Helper()
	buf := new(bytes.Buffer)
	w := parquet.NewGenericWriter[T](buf)
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func readRows[T any](t *testing.T, data []byte, n int) []T {
	t.Helper()
	r := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer r.Close()
	got := make([]T, n)
	k, err := r.Read(got)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if k != n {
		t.Fatalf("want %d rows, got %d", n, k)
	}
	return got
}

// columnValues returns the stored values of one column. Round trips alone
// can't catch a reader and a writer that are wrong in matching ways.
func columnValues(t *testing.T, data []byte, column string) []parquet.Value {
	t.Helper()
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	leaf, ok := f.Schema().Lookup(strings.Split(column, ".")...)
	if !ok {
		t.Fatalf("no column %q", column)
	}
	var values []parquet.Value
	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, rg.NumRows())
		n, err := rows.ReadRows(buf)
		rows.Close()
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		for _, row := range buf[:n] {
			values = append(values, row[leaf.ColumnIndex])
		}
	}
	return values
}

func TestTimestampOutOfNanosRangeConvertValue(t *testing.T) {
	msType := parquet.Timestamp(parquet.Millisecond).Type()
	usType := parquet.Timestamp(parquet.Microsecond).Type()
	dateType := parquet.Date().Type()

	for _, ts := range outOfNanosRangeTimes {
		ts := ts.Truncate(time.Millisecond)
		days := ts.Unix() / 86400
		if ts.Unix()%86400 < 0 {
			days--
		}
		midnight := time.Unix(days*86400, 0).UTC()

		tests := []struct {
			scenario  string
			fromType  parquet.Type
			fromValue parquet.Value
			toType    parquet.Type
			toValue   parquet.Value
		}{
			{
				scenario:  "micros to millis",
				fromType:  usType,
				fromValue: parquet.Int64Value(ts.UnixMicro()),
				toType:    msType,
				toValue:   parquet.Int64Value(ts.UnixMilli()),
			},
			{
				scenario:  "millis to micros",
				fromType:  msType,
				fromValue: parquet.Int64Value(ts.UnixMilli()),
				toType:    usType,
				toValue:   parquet.Int64Value(ts.UnixMicro()),
			},
			{
				scenario:  "micros to date",
				fromType:  usType,
				fromValue: parquet.Int64Value(ts.UnixMicro()),
				toType:    dateType,
				toValue:   parquet.Int32Value(int32(days)),
			},
			{
				scenario:  "date to micros",
				fromType:  dateType,
				fromValue: parquet.Int32Value(int32(days)),
				toType:    usType,
				toValue:   parquet.Int64Value(midnight.UnixMicro()),
			},
		}

		for _, test := range tests {
			t.Run(ts.Format(time.RFC3339Nano)+"/"+test.scenario, func(t *testing.T) {
				got, err := test.toType.ConvertValue(test.fromValue, test.fromType)
				if err != nil {
					t.Fatal(err)
				}
				if !parquet.DeepEqual(test.toValue, got) {
					t.Errorf("want %+v, got %+v", test.toValue, got)
				}
			})
		}
	}
}

// The stored values have to be right too, not just the round trip.
func TestTimestampOutOfNanosRangePhysicalValues(t *testing.T) {
	type row struct {
		Millis time.Time `parquet:"millis,timestamp(millisecond)"`
		Micros time.Time `parquet:"micros,timestamp(microsecond)"`
	}
	rows := make([]row, len(outOfNanosRangeTimes))
	for i, ts := range outOfNanosRangeTimes {
		rows[i] = row{Millis: ts, Micros: ts}
	}
	data := writeRows(t, rows)
	millis, micros := columnValues(t, data, "millis"), columnValues(t, data, "micros")
	for i, ts := range outOfNanosRangeTimes {
		if got, want := millis[i].Int64(), ts.UnixMilli(); got != want {
			t.Errorf("millis %d: want %d, got %d", i, want, got)
		}
		if got, want := micros[i].Int64(), ts.UnixMicro(); got != want {
			t.Errorf("micros %d: want %d, got %d", i, want, got)
		}
	}
}

// Nanoseconds only reach 1677-2262, so this checks the fix leaves that unit alone.
func TestTimestampNanosecondRoundTrip(t *testing.T) {
	type row struct {
		Value   time.Time  `parquet:"value,timestamp(nanosecond)"`
		Pointer *time.Time `parquet:"pointer,optional,timestamp(nanosecond)"`
	}
	times := []time.Time{
		time.Date(1677, 9, 22, 0, 0, 0, 0, time.UTC),
		time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 1, 2, 3, 4, 5, 6, time.UTC),
		time.Date(2262, 4, 10, 0, 0, 0, 0, time.UTC),
	}
	rows := make([]row, len(times))
	for i, ts := range times {
		ts := ts
		rows[i] = row{Value: ts, Pointer: &ts}
	}
	got := roundTripRows(t, rows)
	for i, ts := range times {
		if !got[i].Value.Equal(ts) {
			t.Errorf("value %d: want %v, got %v", i, ts, got[i].Value)
		}
		if got[i].Pointer == nil || !got[i].Pointer.Equal(ts) {
			t.Errorf("pointer %d: want %v, got %v", i, ts, got[i].Pointer)
		}
	}
}

// A coarser unit floors, as time.Time.UnixMilli does, so a time just before
// 1970 stays in 1969. v0.32.0 truncated -1µs to 0ms.
func TestTimestampConvertFloorsBeforeEpoch(t *testing.T) {
	msType := parquet.Timestamp(parquet.Millisecond).Type()
	usType := parquet.Timestamp(parquet.Microsecond).Type()
	got, err := msType.ConvertValue(parquet.Int64Value(-1), usType)
	if err != nil {
		t.Fatal(err)
	}
	if want := parquet.Int64Value(-1); !parquet.DeepEqual(want, got) {
		t.Errorf("want %+v, got %+v", want, got)
	}
}
