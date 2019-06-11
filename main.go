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
	if err != nil {
        	log.Fatalf("Unmarshal: %v", err)
		return nil, errors.New(fmt.Sprintf("Unmarshal: %v\n", err))
    	}

	return conf, nil
}


func main() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
	conf, err := ReadConfig()
	if err != nil {
		log.Printf("call ReadConfig() function error: %s\n", err)
	}

	var oracleConfig map[string]string
	oracleConfig["host"] =conf.Oracle.Host
	oracleConfig["port"] =  string(conf.Oracle.Port)
	oracleConfig["username"] = conf.Oracle.UserName
	oracleConfig["password"] = conf.Oracle.Password
	oracleConfig["defaultdb"]  = conf.Oracle.Db

	ora := oraclelib.NewOracle(oracleConfig)
	var querySql = `select * from TRANSFER_CARGO`
	results := ora.Read1(querySql)
	for idx,row := range results{
		fmt.Printf("idx=%d, idx=%s\n",idx, row)
	}
}

