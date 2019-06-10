package oraclelib

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"log"
	"strconv"
)

type IOracle interface {
	Read(sql string) []map[string]string
	Read1(sql string) [][]string
	Write(sql string) error
}

type oracle struct {
	conn *sql.DB
}

func NewOracle(conf map[string]string) IOracle {

	user := conf["username"]
	password := conf["password"]
	host := conf["host"]
	port, _ := strconv.Atoi(conf["port"])
	defaultdb := conf["defaultdb"]
	dsn := fmt.Sprintf("%s/%s@%s:%d/%s",
		user,
		password,
		host,
		port,
		defaultdb,
	)

	db, err := sql.Open("oci8", dsn)
	if err != nil {
		fmt.Println("abc", 123, err)
		return nil
	}
	//defer db.Close()
	if err = db.Ping(); err != nil {
		fmt.Printf("Error connecting to the database: %s\n", err)
		return nil
	}

	return &oracle{
		conn: db,
	}
}

func (o *oracle) Read1(sqlStr string) [][]string {
	fmt.Println(sqlStr)
	rows, err := o.conn.Query(sqlStr)
	defer rows.Close()
	if err != nil {
		log.Fatalf("query error %s\n err: %s\n", sqlStr, err)
	}
	columns, err := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	scans := make([]interface{}, len(columns))

	for i := range values {
		scans[i] = &values[i]
	}

	var results [][]string
	for rows.Next() {
		_ = rows.Scan(scans...)
		var line []string
		for _, col := range values {
			line = append(line, string(col))
		}
		//each := make(map[string]string)
		//for i, col := range values{
		//	each[columns[i]] = string(col)
		//}
		results = append(results, line)
	}
	return results
}

func (o *oracle) Read(sqlStr string) []map[string]string {
	fmt.Println(sqlStr)
	rows, err := o.conn.Query(sqlStr)
	defer rows.Close()
	if err != nil {
		log.Fatalf("query error %s\n err: %s\n", sqlStr, err)
	}
	columns, err := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	scans := make([]interface{}, len(columns))

	for i := range values {
		scans[i] = &values[i]
	}

	var results []map[string]string
	for rows.Next() {
		_ = rows.Scan(scans...)
		each := make(map[string]string)
		for i, col := range values {
			each[columns[i]] = string(col)
		}
		results = append(results, each)
	}
	return results
}

func (o *oracle) Write(sql string) (err error) {
	defer o.conn.Close()
	res, err := o.conn.Exec(sql)
	if err != nil {
		log.Println(err)
	}
	log.Println(res.LastInsertId())
	err = nil
	return
}

type CargoInfo struct {
	WeekFirstDay string
	StoreNo      string
	ProductCode  string
	SizeFieldSeq string
	BefInventory string
	AftInventory string
}

var (
	User      string
	Passwd    string
	Host      string
	Port      string
	DefaultDb string
)

//func OpenDb(conf *types.Config) (*sql.DB, error) {
//	var dsn = `%s/%s@%s:%d/%s`
//	dsn = fmt.Sprintf(dsn, conf.Oracle.UserName,
//	conf.Oracle.Password,
//	conf.Oracle.Host,
//	conf.Oracle.Port,
//	conf.Oracle.Db,
//	)
//	fmt.Printf("dsn:%s\n", dsn)
//	db, err := sql.Open("oci8", dsn)
//	if err != nil {
//		fmt.Printf("open oracle error: %s\n", err)
//		return nil, errors.New(fmt.Sprintf("open oracle error: %s\n", err))
//	}
//	if err = db.Ping(); err != nil {
//		fmt.Printf("Error connecting to the database: %s\n", err)
//		return nil, err
//
//	}
//	return db, nil
//}

func main1122() {

	// db, err := sql.Open("oci8", "bi_read/readonly@10.240.20.141:1521/report_141")
	//db, err := sql.Open("oci8", "u_md_rs_pmp/wonhigh_pmp@2019@10.240.9.241:1521/bi_edw_shoes")
	//if err != nil {
	//	fmt.Println("abc", 123, err)
	//	return
	//}
	//defer db.Close()
	//if err = db.Ping(); err != nil {
	//	fmt.Printf("Error connecting to the database: %s\n", err)
	//	return
	//}

	//ora := NewOracle(db)
	//
	//var sql = "select * from transfer_cargo"
	//result := ora.Read1(sql)
	//fmt.Println(result)
	//
	//var cargoinfo1 = &CargoInfo{
	//	"2019-01-01",
	//	"IAJ013",
	//	"SZP9E504DP1AQ9|IAJ013",
	//	"220,225,230,235,240,245",
	//	"1,0,1,0,2,0",
	//	"2,1,2,1,3,1",
	//}
	//var sql_i = `insert into transfer_cargo(ID,week_first_day,store_no,product_code,size_fields_seq,b_inventory,a_inventory)
	//			 values(seq_cargo.nextval, '%s', '%s', '%s', '%s', '%s', '%s')`
	//sql_i = fmt.Sprintf(sql_i, cargoinfo1.WeekFirstDay, cargoinfo1.StoreNo, cargoinfo1.ProductCode, cargoinfo1.SizeFieldSeq, cargoinfo1.BefInventory, cargoinfo1.AftInventory)
	//ora.Write(sql_i)
}
