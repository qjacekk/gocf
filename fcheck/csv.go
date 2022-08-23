package fcheck

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strconv"
)

const (
	N_SAMPLE_ROWS = 10
	Nullstr = "null"
)
var FloatPattern = regexp.MustCompile(`^[-+]?[\d]+\.[\d]*([eE][-+]?[\d]+)?$`)
//var IntPattern = regexp.MustCompile(`^[0#]?[x]?[0-9a-fA-F]+$`)
var IntPattern = regexp.MustCompile(`^[-+]?\d+$`)

type CsvReader struct {
	fileName string
	delimiter rune
	hasHeader bool
	fields []string
	types []DataType
	tmpRow []any
}
func NewCsvReader(fileName string, delimiter rune) CsvReader {
	return CsvReader{fileName:fileName, delimiter:delimiter, hasHeader:true}
}
func (cr *CsvReader) FileName() string {
	return cr.fileName
}

func (cr *CsvReader) toList(values []string) []any {
	for i,sv := range values {
		switch cr.types[i] {
		case DT_float:
			if v,err := strconv.ParseFloat(sv, 64); err == nil {
				cr.tmpRow[i] = v
				continue
			}
		case DT_int:
			if v,err := strconv.ParseInt(sv, 10, 64); err == nil {
				cr.tmpRow[i] = v
				continue
			}
		}
		cr.tmpRow[i] = sv
	}
	return cr.tmpRow
}

func sniffCsvSample(sample [][]string) (hasHeader bool, fields []string, types []DataType){
	hasHeader = true
	nRows := len(sample)
	nFields := len(sample[0])
	var sTypes[][]DataType = make([][]DataType, nRows)
	for r:=0; r< nRows; r++ {
		row := sample[r]
		if r == 0 { // check header
			for _,s := range row {
				if len(s)==0 { // definetely not a header
					hasHeader = false  // hasHeader is initialized to true by default
					break
				}
			}
		}
		// sniff types
		sTypes[r] = make([]DataType, nFields)
		for c,s := range row {
			thisType := DT_string
			if len(s) > 0 {
				if FloatPattern.MatchString(s) {
					if _,err := strconv.ParseFloat(s, 64); err == nil {
							thisType = DT_float
					}
				} else if IntPattern.MatchString(s) {
						if _,err := strconv.ParseInt(s, 10, 64); err == nil {
							thisType = DT_int
						}
				}
				sTypes[r][c] = thisType
			}
		}
	}
	types = make([]DataType, nFields)
	// for each column, check if types are consistent
	for c:=0; c<nFields; c++ {
		headerType := sTypes[0][c]
		valType := sTypes[1][c]
		for r:=2; r<nRows; r++ {
			thisType := sTypes[r][c] 
			if thisType != DT_unknown {
				if sTypes[r][c] != valType {
					// mixed types
					if((valType == DT_float || valType == DT_int) &&  (thisType == DT_float || thisType == DT_int)) {
						// for mixed ints and floats stay with the floats
						valType = DT_float
					} else {
						// otherwise set to string and leave
						valType = DT_string
						break
					}
				} else if valType == DT_unknown { 
					// in case row no.1 was empty
					valType = thisType
				}
			}
		}
		if valType == DT_unknown { valType = DT_string }
		if valType != DT_string && valType == headerType {
			hasHeader = false
		}
		types[c] = valType
	}
	if hasHeader {
		fields = sample[0]
	} else {
		fields = make([]string, nFields)
		for i:=0; i<nFields; i++ {
			fields[i] = fmt.Sprintf("c_%d", i)
		}
	}
	return
}

func (cr *CsvReader) Init() {
	// read first few lines of the csv to get the fields and types
	f, err := os.Open(cr.fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	csvReader := csv.NewReader(f)
	csvReader.Comma = cr.delimiter

	// sniff sample, take N_SAMPLE_ROWS first lines
	var sample [][]string
	for i:=0; i< N_SAMPLE_ROWS; i++ {
		row, err := csvReader.Read()
		if err != nil {
			log.Fatal(err)
		}
		sample = append(sample, row)
	}
	cr.hasHeader, cr.fields, cr.types = sniffCsvSample(sample)
	// init reusable row
	cr.tmpRow = make([]any, len(cr.fields))
}

func (cr *CsvReader) GetFields() []string {
	return cr.fields
}
func (cr *CsvReader) GetTypes() []DataType {
	return cr.types
}
func (cr *CsvReader) GetFileInfo() string {
	return fmt.Sprintf("CSV, %d columns, delimited with '%c'", len(cr.fields), cr.delimiter)
}
func (cr *CsvReader) Read() chan []any {
	out := make(chan []any)
	go func(inFile string) { // equivalent to python's generator
		f, err := os.Open(inFile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		
		// TODO:
		//fields, types, delimiter := inferCsvFormat(f)

		csvReader := csv.NewReader(f)
		csvReader.Comma = cr.delimiter
		csvReader.ReuseRecord = true
		if cr.hasHeader {
			// skip header
			_, err := csvReader.Read()
			if err == io.EOF {
				log.Fatal(err)
			}
		}
		for {
			rec, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			out <- cr.toList(rec)
		}
		close(out)
	} (cr.fileName)
	return out
}