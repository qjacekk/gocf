package fcheck

import (
	"fmt"
	"testing"
)
const (
	AVRO_NULL_PATH = "..\\test\\data\\avro_null_codec"
	AVRO_SNAPPY_PATH = "..\\test\\data\\avro_snappy"
	AVRO_NULL_ROWS = 1000
	AVRO_SNAPPY_ROWS = 1000
)

func TestAvroReaderNullCodec(t *testing.T) {
	fr := NewAvroReader(AVRO_NULL_PATH)
	fr.Init()
	if fr.compression != "null" {
		t.Errorf("Invalid codec: %s", fr.compression)
	}
	fmt.Println("fields:", fr.GetFields())
	fmt.Println("types:", fr.GetTypes())
	i := 0
	for row := range fr.Read() {
		if len(row) == 0 {
			t.Errorf("row %d is empty", i)
		}
		i++
	}
	if i!=AVRO_NULL_ROWS {
		t.Errorf("Expected %d rows, got %d", AVRO_NULL_ROWS, i)
	}
}

func TestAvroReaderSnappy(t *testing.T) {
	fr := NewAvroReader(AVRO_SNAPPY_PATH)
	fr.Init()
	if fr.compression != "snappy" {
		t.Errorf("Invalid codec: %s", fr.compression)
	}
	fmt.Println("fields:", fr.GetFields())
	fmt.Println("types:", fr.GetTypes())
	i := 0
	for row := range fr.Read() {
		if len(row) == 0 {
			t.Errorf("row %d is empty", i)
		}
		i++
	}
	if i!=AVRO_SNAPPY_ROWS {
		t.Errorf("Expected %d rows, got %d", AVRO_SNAPPY_ROWS, i)
	}
}

func TestAvroTF(t *testing.T) {
	fr := NewAvroReader(AVRO_NULL_PATH)
	TestFile(&fr, true, 10, false)
}

func TestAvroTFSnappy(t *testing.T) {
	fr := NewAvroReader(AVRO_SNAPPY_PATH)
	TestFile(&fr, true, 10, false)
}
