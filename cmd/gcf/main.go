package main

import (
	"flag"
	"fmt"
	"gocf/fcheck"
	"log"
)


func main() {
	var pNoSort = flag.Bool("ns", false, "do not sort fields alphabetically in the report (use the original file order)")
	var pLeastFreq = flag.Bool("lf", false, "print least frequent samples (default: most frequent")

	var pNoOfSamples = flag.Int("m", 5, "number of sample values to include in the report (default 5)")
	
	//var pToJson = flag.Bool("j", false, "convert to JSON (instead of generating coverage report")
	//var pToCsv = flag.Bool("c", false, "convert to CSV (instead of generating coverage report")
	var pQuoteCsv = flag.Bool("q", false, "enable quoting strings (only if -c was specified, this may slow things down)")
	var pCsvDelimiter = flag.String("d", "", "CSV delimiter (if not specified ftest will try to guess)")
	//var pNumOfRows = flag.Int("n", -1, "number of rows in CSV or JSON output (all by default")
	// TODO: add error handling, add -f option 
	var usage = func () {
		fmt.Fprintln(flag.CommandLine.Output(), "Generate coverage and data validity report.")
		fmt.Fprintln(flag.CommandLine.Output(), "usage: gcf [options] <file_name>")
		fmt.Fprintln(flag.CommandLine.Output(), "Options:")
		flag.PrintDefaults()
	}	
	flag.Usage = usage

	flag.Parse()

	if flag.NArg() > 0 {
		var inputFileName string = flag.Arg(0)
		//fmt.Println(inputFileName)
		//fmt.Println("#### args:", *pNoSort, *pLeastFreq, *pNoOfSamples, *pToJson, *pToCsv, *pQuoteCsv, *pCsvDelimiter, *pNumOfRows)
		reader, err := fcheck.NewFileReader(inputFileName, *pNoSort, *pLeastFreq, *pNoOfSamples, *pQuoteCsv, *pCsvDelimiter)
		if err != nil {
			log.Fatal(err)
		}
		fcheck.TestFile(reader, !*pNoSort, *pNoOfSamples, *pLeastFreq)
	} else {
		usage()
	}
}
