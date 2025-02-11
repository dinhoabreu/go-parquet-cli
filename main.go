package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

// Show the schema of the Parquet file
func showSchema(filePath string) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatalf("Error creating Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	fmt.Println("Parquet File Schema:")
	for _, col := range pr.SchemaHandler.ValueColumns {
		fmt.Println("-", cleanColumnName(col))
	}
}

// Clean column name by removing "Schema\x01" prefix
func cleanColumnName(name string) string {
	prefix := "Schema\x01"
	if strings.HasPrefix(name, prefix) {
		return name[len(prefix):]
	}
	return name
}

// Read Parquet file with given offset, size, and selected columns
func readParquet(filePath string, offset, size int, columns []string) {
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		log.Fatalf("Error creating Parquet reader: %v", err)
	}
	defer pr.ReadStop()

	// Process available columns
	availableColumns := make([]string, len(pr.SchemaHandler.ValueColumns))
	for i, col := range pr.SchemaHandler.ValueColumns {
		availableColumns[i] = cleanColumnName(col)
	}
	if len(columns) == 0 {
		columns = availableColumns
	} else {
		validCols := make(map[string]bool)
		for _, col := range availableColumns {
			validCols[col] = true
		}
		for _, col := range columns {
			if _, exists := validCols[col]; !exists {
				log.Fatalf("Error: Column '%s' does not exist in the Parquet file. %v, %v", col, validCols, validCols[col])
			}
		}
	}

	// Validate row limits
	numRows := int(pr.GetNumRows())
	if offset >= numRows {
		log.Fatalf("Error: Offset (%d) is greater than the number of records (%d).", offset, numRows)
	}
	if offset+size > numRows {
		size = numRows - offset
	}

	// Create CSV writer
	writer := csv.NewWriter(os.Stdout)
	defer writer.Flush()

	writer.Write(columns)

	// Batch size configuration
	batchSize := 100
	if size < batchSize {
		batchSize = size
	}

	// Read records in batches
	for i := offset; i < offset+size; i += batchSize {
		toRead := batchSize
		if (offset + size - i) < batchSize {
			toRead = offset + size - i
		}

		// Skip rows before starting the read
		if i == offset && offset > 0 {
			if err := pr.SkipRows(int64(offset)); err != nil {
				log.Fatalf("Error skipping %d rows: %v", offset, err)
			}
		}

		// Create buffer to store column data
		columnData := make([][]interface{}, len(columns))

		for j, col := range columns {
			numValues := int64(toRead)
			values, _, _, err := pr.ReadColumnByPath("Schema\x01"+col, numValues)
			if err != nil {
				log.Fatalf("Error reading column %s: %v", col, err)
			}
			columnData[j] = values // Store values correctly
		}

		// Write values to CSV correctly
		for row := 0; row < toRead; row++ {
			var record []string
			for _, colValues := range columnData {
				record = append(record, fmt.Sprintf("%v", colValues[row]))
			}
			writer.Write(record)
		}
	}
}

func main() {
	// Define CLI flags
	filePath := flag.String("file", "", "Path to the Parquet file (required)")
	schemaOnly := flag.Bool("schema", false, "Display only the Parquet schema")
	offset := flag.Int("offset", 0, "Start row number")
	size := flag.Int("size", 10, "Number of rows to read")
	columns := flag.String("columns", "", "List of columns separated by commas (e.g., 'col1,col2,col3')")

	// Custom help message function
	flag.Usage = func() {
		fmt.Println("Usage: go run main.go --file <parquet_file> [options]")
		fmt.Println("\nAvailable options:")
		flag.PrintDefaults()
		fmt.Println("\nUsage examples:")
		fmt.Println("  - Display the schema of a Parquet file:")
		fmt.Println("    go run main.go --file data.parquet --schema")
		fmt.Println("\n  - Read records from a Parquet file:")
		fmt.Println("    go run main.go --file data.parquet --offset 10 --size 20 --columns col1,col2")
		fmt.Println("\n  - Display help:")
		fmt.Println("    go run main.go --help")
	}

	// Parse arguments
	flag.Parse()

	// Show help if no arguments are provided
	if flag.NFlag() == 0 {
		flag.Usage()
		os.Exit(0)
	}

	// Validate that the file parameter was provided
	if *filePath == "" {
		log.Fatal("Error: The --file parameter is required.")
	}

	// Show schema if requested
	if *schemaOnly {
		showSchema(*filePath)
		return
	}

	// Convert column string to a slice
	var selectedColumns []string
	if *columns != "" {
		selectedColumns = strings.Split(*columns, ",")
	}

	// Read the Parquet file with the provided options
	readParquet(*filePath, *offset, *size, selectedColumns)
}
