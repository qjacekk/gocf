package fcheck

import (
	"bytes"
	"errors"
	"fmt"
	"gocf/fcheck/stats"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// FileReader interface, common methods that must be implemented for each indivitual file type reader:
// Init() - called just after an instance has been created, this should be used to read the schema, 
//        find out what are the fields and their types.
// GetFields() - returns filed names (before the first call to Read()!), order of the fields matters!
// GetTypes() -  returns types of the fields (must match the fields order)
// GetFileInfo() - a one line description of the file (type, size, compression codec etc.)
// Read() - returns channel to read rows. A row is a slice of any values but the size and order must match fields and types
type FileReader interface {
	FileName() string
	Init()
	GetFields() [] string
	GetTypes()  [] DataType
	GetFileInfo() string
	Read()      chan []any
}

// Enum FileType specifies different file types like csv, avro etc.
type FileType uint
const(
	FT_unknown FileType = iota
	FT_avro
	FT_csv
	FT_json
	FT_parquet
)

type DataType uint
const(
	DT_unknown DataType = iota
	DT_string
	DT_int
	DT_float
)
func (s DataType) String() string {
	switch s {
	case DT_string:
		return "string"
	case DT_int:
		return "int"
	case DT_float:
		return "float"
	}
	return "unknown"
}

// Magic bytes constants (byte arrays can't be declared as consts)
var (
	MAGIC_PAR = []byte("PAR1")
	MAGIC_AVRO = []byte{79, 98, 106,1}
)

// TODO: move all helpers to util.go
// helper functions 
func indexof(array []string, value string) int {
	for i,s:= range array {
		if s == value {
			return i
		}
	}
	return -1
}

func getStatCollectors(types []DataType, noOfMostFrequentValues int) ([]stats.StatCollector, bool) {
	statCollectors := make([]stats.StatCollector, len(types))
	anyString := false
	for i,t := range types {
		switch t {
		case DT_float, DT_int:
			statCollectors[i] = &stats.RunningStats{}
		default:
			statCollectors[i] = stats.NewStringFreq()
			anyString = true		
		}
	}
	return statCollectors, anyString
}


func TestFile(fr FileReader, sorted bool, noOfMostFrequentValues int, leastFreuquent bool) {
	fr.Init()
	fields := fr.GetFields()
	types := fr.GetTypes()
	statCollectors, anyString := getStatCollectors(types, noOfMostFrequentValues)
	noOffields := len(fields)
	rowCount := 0
	start := time.Now()
	for row := range fr.Read() {
		rowCount++
		for i:=0; i<noOffields; i++ {
			value := row[i]
			statCollectors[i].Push(value)
		} 
	}	
	end := time.Now()
	fmt.Println("File:", fr.FileName())
	fmt.Println("Info:", fr.GetFileInfo())


	if rowCount < 1 {
		fmt.Println("No data found")
		return
	}

	sortedfields := fields[:] // make a slice first
	if sorted {
		sort.Strings(sortedfields)
	}


	fmt.Println("=================")
	fmt.Println(" coverage report ")
	fmt.Println("=================")

	maxFieldLen := 30;
	for _,field := range sortedfields {
		if len(field) > maxFieldLen {
			maxFieldLen = len(field)
		}
	}
	smaxFieldLen := strconv.Itoa(maxFieldLen)
	// print header
	headerTemplate := "%-"+ smaxFieldLen +"s : %-8s : %-6s : %-16s : %s"
	template := "%-"+ smaxFieldLen +"s : %-8d : %-6.2f : %-16s : %s\n"
	h1 := fmt.Sprintf(headerTemplate, "field", "count", "%", "type", "comment")
	fmt.Println(h1);
	fmt.Println(strings.Repeat("-", len(h1)))
	
	for _,field := range sortedfields {
		i := indexof(fields, field)
		s := statCollectors[i]
		fmt.Printf(template, field, s.Count(), float64(100*s.Count()) / float64(rowCount), types[i], s.Info())
	}
	fmt.Println()

	// stats for numerical or n  most/least frequent values for categorical
	if anyString {
		var title2 string
		if leastFreuquent {
			title2 = fmt.Sprintf("%d least frequent string values", noOfMostFrequentValues)
		} else {
			title2 = fmt.Sprintf("%d most frequent string values", noOfMostFrequentValues) 
		}
		fmt.Println(title2)
		fmt.Println(strings.Repeat("=", len(title2)))
	
		header2Template :=    "%-"+ smaxFieldLen +"s : %-8s : %-6s : %-16s"
		h2 := fmt.Sprintf(header2Template, "field", "count", "%", "value");
		fmt.Println(h2);
		fmt.Println(strings.Repeat("-", len(h2)))
		template2 := "%-"+ smaxFieldLen +"s : %-8d : %-6.2f : "

		for i,field := range sortedfields {
			if types[i] != DT_string {
				continue
			}
			j := indexof(fields, field)
			s := statCollectors[j]
			
			fmt.Println(field);
			if (s.Count() == 0) {
				fmt.Printf("%-"+ strconv.Itoa(maxFieldLen) +"s : %s\n","","--- NOT AVAILABLE ---")
			} else {
				vals, counts := s.Freq(noOfMostFrequentValues, leastFreuquent)
				for k:=0; k<len(vals); k++ {
					fmt.Printf(template2, "", counts[k], float64(100*counts[k])/float64(rowCount))
					fmt.Println(vals[k])
				}
			}
		}
	}
	fmt.Printf("Done in %.3f seconds.\n", end.Sub(start).Seconds())
}

func ToCsv(fr *FileReader) { 
	// TODO: implement
}
func ToJson(fr *FileReader) {
	// TODO: implement
}

func inferFileType(fileName string, delimiter string) FileType {
	f, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		// check byte magic for binary file types
		var mbuff = make([]byte, 4)
		i,err := f.Read(mbuff)
		if (err != nil || i!=4) {
			log.Fatal("Error reading "+fileName, err)
		}
		if bytes.Equal(mbuff, MAGIC_PAR) {
			return FT_parquet
		}
		if bytes.Equal(mbuff, MAGIC_AVRO) {
			return FT_avro
		}
		// if delimiter is specified assume CSV
		if(delimiter != "" || strings.HasSuffix(fileName, ".csv")) {
			return FT_csv
		}
		if(strings.HasSuffix(fileName, ".json")) {
			return FT_json
		}
		return FT_unknown
}

func NewFileReader(fileName string, noSort bool, leastFreq bool, noOfSamples int, quoteCsv bool, csvDelimiter string) (FileReader, error) {
	inferedType := inferFileType(fileName, csvDelimiter)
	switch inferedType {
		// TODO: add more readers
	case FT_csv:
		delimiter := ','
		if len(csvDelimiter) > 0 {
			delimiter = rune(csvDelimiter[0])
		}
		c := NewCsvReader(fileName, delimiter)
		return &c, nil
	case FT_avro:
		c := NewAvroReader(fileName)
		return &c, nil
	default:
		return nil, errors.New("unknown file format")
	}
}
