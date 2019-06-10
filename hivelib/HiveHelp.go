package hivelib

import (
	"context"
	"fmt"
	"github.com/beltran/gohive"
	"os"
	"strconv"

	"log"
)

type IHive interface {
	CloseHive()
	Read(string) []map[string]interface{}
	ReadList(string) [][]string
	Write(string) (int, error)
	DownLoad(string, string) error
}

type hive struct {
	ts *gohive.Connection
}

func NewHive(conf map[string]string) IHive {

	configuration := gohive.NewConnectConfiguration()
	configuration.Service = conf["service"]
	configuration.Username = conf["username"]
	configuration.Password = conf["password"]
	int64, _ := strconv.ParseInt(conf["fetchsize"], 10, 64)
	configuration.FetchSize = int64
	port, _ := strconv.Atoi(conf["port"])
	conn, connErr := gohive.Connect(conf["host"], port, "NONE", configuration)
	if connErr != nil {
		log.Fatalf("Connect error:%s\n", connErr)
		os.Exit(1)
	}
	return &hive{
		ts: conn,
	}
}

func (h *hive) ReadLine(Querysql string) {

}

func (h *hive) CloseHive() {
	h.ts.Close()
}

func (h *hive) Write(InsertSql string) (eff_rows int, err error) {
	cursor := h.ts.Cursor()
	defer cursor.Close()
	ctx := context.Background()
	cursor.Execute(ctx, InsertSql, false)
	cursor.WaitForCompletion(ctx)
	if cursor.Err != nil {
		log.Fatalf("Insert into hive:%s\n", cursor.Err)
	}

	return 1, nil
}

func (h *hive) DownLoad(filepath, FileType string) (err error) {
	switch FileType {
	case "csv":
		fmt.Println(FileType)
	case "txt":
		fmt.Println(FileType)
	case "xlsx":
		fmt.Println(FileType)
	case "xls":
		fmt.Println(FileType)
	default:
		fmt.Println(FileType)
	}
	fmt.Println("download")
	return nil
}

func (h *hive) ReadList(QuerySql string) [][]string {
	ctx := context.Background()
	cursor := h.ts.Cursor()
	defer cursor.Close()
	var results [][]string
	//cursor.Execute(ctx, QuerySql, true)
	cursor.Exec(ctx, QuerySql)
	columns := cursor.Description()
	scans := make([]interface{}, len(columns))
	values := make([]string, len(columns))
	for i := range values {
		scans[i] = &values[i]
	}
	for cursor.HasMore(ctx) {
		if cursor.Err != nil {
			log.Fatalf("cursor error:%s\n", cursor.Err)
		}
		cursor.FetchOne(ctx, scans...)
		results = append(results, values)
	}
	return results
}

func (h *hive) Read(QuerySql string) []map[string]interface{} {
	ctx := context.Background()
	cursor := h.ts.Cursor()
	defer cursor.Close()
	cursor.Exec(ctx, QuerySql)
	if cursor.Err != nil {
		log.Fatal(cursor.Err)
	}

	var results []map[string]interface{}
	for cursor.HasMore(ctx) {
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		line := cursor.RowMap(ctx)
		if cursor.Err != nil {
			log.Fatal(cursor.Err)
		}
		results = append(results, line)
	}
	return results
}

func Engine() (err error){
	return nil
}

//
//func main() {
//	conf := map[string]string{"service": "hive", "host": "10.240.20.20", "port": "10000", "username": "zheng.xf", "password": "zheng.xf", "fetchsize": "9999"}
//	h := NewHive(conf)
//	var Query1 string
//	Query1 = ` SELECT
//				CAST(product_code AS string) AS product_code ,
//				CAST(store_no AS string) AS store_no,
//				CAST(sum_qty_2week_220 AS string) AS sum_qty_2week_220
//			FROM
// 				belle_sh.yl_trans_by_SKU_sale_inv_20190602_spr`
//	r := h.ReadList(Query1)
//	fmt.Println(r)
//	h.CloseHive()
//}
