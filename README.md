# Go Parquet CLI

This CLI tool allows you to read and extract data from Apache Parquet files with flexible options such as column selection, offset, and batch size. It also provides functionality to display the schema of a Parquet file.

## Features

- Display Parquet schema with --schema
- Extract data as CSV
- Select specific columns
- Set offset and batch size for efficient processing
- User-friendly CLI with --help

## Installation

### Clone the repository

```sh
git clone https://github.com/dinhoabreu/go-parquet-cli.git
cd go-parquet-cli
```

### Install dependencies

```sh
go mod tidy
```

### Run the CLI

```sh
go run main.go --help
```

## Usage

### Display the schema of a Parquet file

```sh
go run main.go --file dat/weather.parquet --schema
```

Example output:

```txt
Parquet File Schema:
- Cloud3pm
- MinTemp
- MaxTemp
- Humidity9am
```

### Read data from a Parquet file

```sh
go run main.go --file dat/weather.parquet --offset 10 --size 20 --columns Cloud3pm,MinTemp
```

This command extracts 20 rows, starting from row 10, only for the columns Cloud3pm and MinTemp.

### Read entire file

```sh
go run main.go --file dat/weather.parquet --size 100
```

This reads 100 rows from the beginning.

### Help Command

```sh
go run main.go --help
```

Output:

```txt
Usage: go run main.go --file <parquet_file> [options]

Available options:
  -columns string
        List of columns separated by commas (e.g., 'col1,col2,col3')
  -file string
        Path to the Parquet file (required)
  -offset int
        Start row number
  -schema
        Display only the Parquet schema
  -size int
        Number of rows to read (default 10)

Usage examples:
  - Display the schema of a Parquet file:
    go run main.go --file data.parquet --schema

  - Read records from a Parquet file:
    go run main.go --file data.parquet --offset 10 --size 20 --columns col1,col2

  - Display help:
    go run main.go --help
```

### Building the Binary

To build a standalone binary:

```sh
go build -o bin/parquet-cli main.go
```

Then, you can run it:

```sh
./bin/parquet-cli --file dat/weather.parquet --schema
```

### Dependencies

- [parquet-go](https://github.com/xitongsys/parquet-go) - Apache Parquet reader for Go

### License

This project is licensed under the MIT License.
