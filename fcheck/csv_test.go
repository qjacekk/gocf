package fcheck

import (
	"fmt"
	"testing"
)
const (
	CSV_SIMPLE_ROWS = 1000
)
func TestCsvReader(t *testing.T) {
	cr := NewCsvReader("..\\test\\data\\simple.csv", ',')
	cr.Init()
	fmt.Println("fields:", cr.GetFields())
	fmt.Println("types:", cr.GetTypes())
	i := 0
	for row := range cr.Read() {
		if len(row) == 0 {
			t.Errorf("row %d is empty", i)
		}
		i++
	}
	if i!=CSV_SIMPLE_ROWS {
		t.Errorf("Expected %d rows, got %d", CSV_SIMPLE_ROWS, i)
	}
}

func TestCsvSniffer(t *testing.T) {
	sample := [][]string {
		{"h_str", "h_int",                 "h_float",                                        "h_mix"},
		{"row1",  "1",                     "1",                                               "0.01"},
		{"row2",  "-123",                  "-123.4567",                                       "1"},
		{"row3",  "9223372036854775807",  "1.797693134862315708145274237317043567981e+308",  "abc"},
		{"row4",  "-9223372036854775807", "4.940656458412465441765687928682213723651e-324",  "-12"},
		{"row5",  "012",                   "-1.797693134862315708145274237317043567981e+308", "0x123"},
		{"row6",  "0001",                  "-4.940656458412465441765687928682213723651e-324", "ABC"},
		{"",  "",     "",    ""},
		{"row8",  "4294967295",            "0.001",                                             "123"},
		{"row9",  "-4294967295",           "0.123e-45",                                         "0.01"},
		{"row10", "-0123",                 "-3.40282346638528859811704183484516925440e+38",    "4561"},
		{"row11", "-456",                 "-1.401298464324817070923729583289916131280e-45",   "0.01"},
		{"row12", "000",                   "-0.123",                                            "0.01"},
	}
	hasHeader, fields, types := sniffCsvSample(sample)
	if !hasHeader {
		t.Error("header not detected")
	}
	expectedTypes := []DataType{DT_string, DT_int, DT_float, DT_string}
	for i,actType := range types {
		if actType != expectedTypes[i] {
			t.Error("types do not match expected:", expectedTypes, "got:", types)
		}
	}
	fmt.Println("hasHeader:", hasHeader)
	fmt.Println("fields   :", fields)
	fmt.Println("types    :", types)
}

func TestCsvSnifferNeg(t *testing.T) {
	sample := [][]string {
		{"row1",  "1",                     "1",                                               "0.01"},
		{"row2",  "-123",                  "-123.4567",                                       "1"},
		{"row3",  "9223372036854775807",  "1.797693134862315708145274237317043567981e+308",  "abc"},
		{"row4",  "-9223372036854775807", "4.940656458412465441765687928682213723651e-324",  "-12"},
		{"row5",  "012",                   "-1.797693134862315708145274237317043567981e+308", "0x123"},
		{"row6",  "0001",                  "-4.940656458412465441765687928682213723651e-324", "ABC"},
	}
	hasHeader, fields, types := sniffCsvSample(sample)
	if hasHeader {
		t.Error("header detected")
	}
	expHeader := []string {"c_0", "c_1", "c_2", "c_3"}
	expectedTypes := []DataType{DT_string, DT_int, DT_float, DT_string}
	for i:=0; i<len(expHeader); i++ {
		actType := expectedTypes[i]
		if actType != expectedTypes[i] {
			t.Error("types do not match expected:", expectedTypes, "got:", types)
		}
		actName := fields[i]
		if actName != expHeader[i] {
			t.Error("fields do not match expected:", expHeader, "got:", fields)
		}

	}
	fmt.Println("hasHeader:", hasHeader)
	fmt.Println("fields   :", fields)
	fmt.Println("types    :", types)
}
