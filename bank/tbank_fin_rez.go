package bank

import (
	"bytes"
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/kmlebedev/clickhouse-import-rosstat/chimport"
	"github.com/kmlebedev/clickhouse-import-rosstat/util"
	"github.com/unidoc/unipdf/v3/extractor"
	"github.com/unidoc/unipdf/v3/model"
	"github.com/xuri/excelize/v2"
	"io"
	"strconv"
	"strings"
	"time"
)

const (
	// https://cdn.tbank.ru/static/documents/c4ade295-d683-431f-a7ac-4f88c685d548.xlsx
	tbIfrsUrl       = "https://cdn.tbank.ru/static/documents/a27e2a89-0871-4d87-9847-46bce3a0ad3f.pdf"
	tbIfrsTable     = "tbank_group_ifrs"
	tbIfrsDataField = "Новостройка"
)

type TbIfrs struct {
}

func (s *TbIfrs) Name() string {
	return tbIfrsTable
}

func (s *TbIfrs) export() (table *[][]string, err error) {
	var xlsx *excelize.File
	if xlsx, err = util.GetXlsx(tbIfrsUrl); err != nil {
		return nil, err
	}
	defer xlsx.Close()
	table = new([][]string)
	var rows [][]string
	if rows, err = xlsx.GetRows("Interest"); err != nil {
		return nil, err
	}
	fieldFound := 0
	// Строки с годами
	for i, row := range rows {
		if len(row) < 2 {
			continue
		}
		if strings.TrimSpace(row[2]) == tbIfrsDataField {
			fieldFound = i - 1
		}
		if fieldFound == 0 {
			continue
		}
		if len(row) < 3 {
			break
		}
		value := strings.TrimSpace(row[3])
		if value == "" || value == "0" || value == "-" {
			continue
		}
		// Колонки с месяцами и пропуском кварталов
		for j, cell := range row[2:] {
			if cell == "" {
				continue
			}
			if j+1 >= len(rows[fieldFound]) {
				break
			}
			// fmt.Printf("name %s date %v cell %s\n", row[1], rows[fieldFound][j+2], cell)
			balance := strings.ReplaceAll(strings.TrimSpace(cell), ",", "")
			if _, err = strconv.ParseFloat(balance, 32); err != nil {
				return nil, err
			}
			*table = append(*table, []string{row[1], rows[fieldFound][j+2], balance})
		}
	}

	return table, nil
}

func (s *TbIfrs) exportPdf() (table *[][]string, err error) {
	reader, err := util.GetFile(tbIfrsUrl)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, _ := io.ReadAll(reader)
	pdfReader, err := model.NewPdfReader(io.NewSectionReader(bytes.NewReader(data), 0, int64(len(data))))
	if err != nil {
		return nil, err
	}
	numPages, err := pdfReader.GetNumPages()
	if err != nil {
		return nil, err
	}
	fmt.Printf("--------------------\n")
	fmt.Printf("PDF to text extraction:\n")
	fmt.Printf("--------------------\n")
	for i := 0; i < numPages; i++ {
		pageNum := i + 1

		page, err := pdfReader.GetPage(pageNum)
		if err != nil {
			return nil, err
		}

		ex, err := extractor.New(page)
		if err != nil {
			return nil, err
		}

		text, err := ex.ExtractText()
		if err != nil {
			return nil, err
		}
		textLines := strings.Split(text, "\n")
		for j, line := range textLines {
			if line == "Баланс Т-Банка по РСБУ," {
				fmt.Printf(textLines[j+1])
			}
		}
		{
		}
		fmt.Println("------------------------------")
		fmt.Printf("Page %d:\n", pageNum)
		fmt.Printf("\"%s\"\n", text)
		fmt.Println("------------------------------")
	}
	table = new([][]string)
	return table, nil
}

func (s *TbIfrs) Import(ctx context.Context, conn driver.Conn) (count int64, err error) {
	if err = conn.Exec(ctx, vtbIfrsTableDdl); err != nil {
		return count, err
	}
	var table *[][]string
	if table, err = s.export(); err != nil {
		return count, err
	}
	for _, row := range *table {
		date, err := time.Parse(vtbIfrsTimeLayout, row[1])
		if err != nil {
			return count, err
		}
		if err = conn.Exec(ctx, vtbIfrsDataInsert, row[0], date, row[2]); err != nil {
			return count, err
		} else {
			count += 1
		}
	}

	return count, nil
}

func init() {
	chimport.Stats = append(chimport.Stats, &TbIfrs{})
}
