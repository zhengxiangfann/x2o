package main

import (
	"errors"
	"fmt"
	_ "github.com/mattn/go-oci8"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"zxf.github.com/x2o/oraclelib"
	"zxf.github.com/x2o/types"
)


func ReadConfig() (*types.Config, error){
	conf := new(types.Config)
	yamlFile, err := ioutil.ReadFile("conf/db.yml")
	log.Println("yamlFile:", string(yamlFile))
	if err  != nil {
		log.Printf("open config file error: #%v", err)
		return nil, errors.New(fmt.Sprintf("open config file error:#%s\n", err))
	}
	err = yaml.Unmarshal(yamlFile, conf)
	// err = yaml.Unmarshal(yamlFile, &resultMap)
	if err != nil {
        	log.Fatalf("Unmarshal: %v", err)
		return nil, errors.New(fmt.Sprintf("Unmarshal: %v\n", err))
    	}
    	// log.Println("conf", conf)
    	// log.Println("conf", conf.Hive.Host)
	return conf, nil
}


func main() {
	// log.SetFlags(log.Lshortfile | log.LstdFlags)
	conf, err := ReadConfig()
	if err != nil {
		log.Printf("call ReadConfig() function error: %s\n", err)
	}

	//
	//db , err := oraclelib.OpenDb(conf)
	//if err != nil {
	//	fmt.Println("call function OpenDb")
	//}
	//fmt.Printf("ora: %s\n", db)
	//
	//

	fmt.Println(*conf)
	var oracleConfig map[string]string
	oracleConfig["host"] =conf.Oracle.Host
	oracleConfig["port"] =  string(conf.Oracle.Port)
	oracleConfig["username"] = conf.Oracle.UserName
	oracleConfig["password"] = conf.Oracle.Password
	oracleConfig["defaultdb"]  = conf.Oracle.Db

	ora := oraclelib.NewOracle(oracleConfig)

	var query_sql = `select * from TRANSFER_CARGO`
	results := ora.Read1(query_sql)
	for idx,row := range results{
		fmt.Printf("idx=%d, idx=%s\n",idx, row)
	}

}

//
//func main11() {
//    // 为log添加短文件名,方便查看行数
//    log.SetFlags(log.Lshortfile | log.LstdFlags)
//    log.Println("Oracle Driver example")
//    //os.Setenv("NLS_LANG", "")
//    // 用户名/密码@IP:端口/实例名
//    db, err := sql.Open("oci8", "goodsman/goodsman@10.240.11.202:1521/retailnc")
//    if err != nil {
//        log.Fatal(err)
//    }
//    rows, err := db.Query("select 3.14, 'foo' from dual")
//    if err != nil {
//        log.Fatal(err)
//    }
//    defer db.Close()
//
//    for rows.Next() {
//        var f1 float64
//        var f2 string
//        rows.Scan(&f1, &f2)
//        log.Println(f1, f2) // 3.14 foo
//    }
//    rows.Close()
//
//    // 先删表,再建表
//    db.Exec("drop table sdata")
//    db.Exec("create table sdata(name varchar2(256))")
//    db.Exec("insert into sdata values('中文')")
//    db.Exec("insert into sdata values('1234567890ABCabc!@#$%^&*()_+')")
//    rows, err = db.Query("select * from sdata")
//    if err != nil {
//        log.Fatal(err)
//    }
//
//    for rows.Next() {
//        var name string
//        rows.Scan(&name)
//        log.Printf("Name = %s, len=%d", name, len(name))
//    }
//    rows.Close()
//}
