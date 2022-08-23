package fcheck

import (
	"fmt"
	"log"
	"os"
	"github.com/hamba/avro"
	"github.com/hamba/avro/ocf"
)

type AvroReader struct {
	fileName string
	file *os.File
	decoder *ocf.Decoder
	schema *avro.RecordSchema
	compression string
	fields []string
	types []DataType
	tmpRow []any
}
func NewAvroReader(fileName string) AvroReader {
	return AvroReader{fileName:fileName}
}
func (ar *AvroReader) toList(record any) []any {
	rec := record.(map[string]any)
	for i,name := range ar.fields {
		switch ar.types[i] {
		case DT_float, DT_int, DT_string:
			ar.tmpRow[i] = rec[name]
		default:
			ar.tmpRow[i] = fmt.Sprintf("%v", rec[name])
		}

	}
	return ar.tmpRow
}
func (ar *AvroReader) FileName() string {
	return ar.fileName;
}

func (ar *AvroReader) Init() {
	// read Avro schema
	f, err := os.Open(ar.fileName)
	if err != nil {
		log.Fatal(err)
	}
	dec, err := ocf.NewDecoder(f)
	if err != nil {
		log.Fatal(err)
	}
	var meta map[string][]byte = dec.Metadata()
	schemaString := string(meta["avro.schema"])
	ar.compression = string(meta["avro.codec"])
	schema, err := avro.Parse(schemaString)
	if err != nil {
		log.Fatal(err)
	}
	if schema.Type() != avro.Record {
		log.Fatal("schema types other than Record are not supported")
	}
	ar.schema = schema.(*avro.RecordSchema)

	nFields := len(ar.schema.Fields())
	ar.fields = make([]string, nFields)
	ar.types = make([]DataType, nFields)
	for i, field := range ar.schema.Fields() {
		ar.fields[i] = field.Name()
		
		fieldSchema := field.Type()  // field.Type() returns Schema
		fieldSchemaType := fieldSchema.Type() // schema.Type() return Type (this is weird API)

		// Fields often are unions of [sometype, null]
		if fieldSchemaType == avro.Union {
			unionSchema := fieldSchema.(*avro.UnionSchema)
			// find not null type
			for _, subSchema := range unionSchema.Types() {
				if subSchema.Type() != avro.Null {
					schema = subSchema
				}
			}
		}
		var typ DataType
		switch schema.Type() {
		case avro.Int, avro.Long:
			typ = DT_int
		case avro.Float, avro.Double:
			typ = DT_float
		default:
			typ = DT_string
		}
		ar.types[i] = typ
	}
	ar.file = f
	ar.decoder = dec
	ar.tmpRow = make([]any, nFields)
}

func (ar *AvroReader) GetFields() []string {
	return ar.fields
}
func (ar *AvroReader) GetTypes() []DataType {
	return ar.types
}
func (ar *AvroReader) GetFileInfo() string {
	return fmt.Sprintf("Avro, %d fields, %s compression", len(ar.fields), ar.compression)
}
func (ar *AvroReader) Read() chan []any {
	out := make(chan []any)
	go func(decoder *ocf.Decoder) { 
		for decoder.HasNext() {
			//var rec any
			var rec map[string]any
			err := decoder.Decode(&rec)
			if err != nil {
				log.Panic(err)
			}
			out <- ar.toList(rec)
		}
		ar.file.Close()
		close(out)
	} (ar.decoder)
	return out
}
