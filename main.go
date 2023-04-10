package main

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Unknwon/goconfig"
	_ "github.com/go-sql-driver/mysql"
	"github.com/wonderivan/logger"
)

// 定义全局变量
var (
	// 数据库连接串
	DBConn string
	// 数据库最大连接数
	MaxConn int
	// 目标地址跟路径
	DestRoot string
	// 目标地址code
	DestCode int
	//设置最大线程数
	MaxThreads int
	//最大任务数
	MaxTasks int
	// 数据通道
	dataChan chan int64
)
var db *sql.DB

func Work(key int64) {
	// 获取dicom和JPG文件路径
	dicom, jpg := getFilePath(key)
	if dicom != "" {
		dicomdestpath := DestRoot + dicom
		logger.Info(key, ": dicom目标文件路径： ", dicomdestpath)
		deleteFile(dicomdestpath)
		updateFileFlag(key)
	}
	if jpg != "" {
		jpgdestpath := DestRoot + jpg
		logger.Info(key, ": jpg目标文件路径： ", jpgdestpath)
		deleteFile(jpgdestpath)
	}
}

func main() {
	// 获取可执行文件的路径
	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	fmt.Println(dir)
	// 配置log
	logger.SetLogger(dir + "/log.json")
	logger.Debug("当前可执行文件路径：" + dir)
	// 读取配置文件
	readConfigFile(dir)
	// instancekey数据通道
	dataChan = make(chan int64)
	// 初始化数据库
	initDB()
	// 处理任务：
	go func() { //这是一个独立的协程 保证可以接受到每个用户的请求
		for {
			select {
			case instance_key := <-dataChan:
				Work(instance_key)
			}
		}
	}()
	// 主程序逻辑
	for {
		// if db.Ping() != nil {
		// 	initDB()
		// }
		// 1.获取instance_key
		getinstancekey()
	}
}

func deleteFile(dest string) {
	if !Exist(dest) {
		logger.Info(dest, "文件不存在.....")
		return
	} else {
		os.Remove(dest)
		logger.Info("文件删除成功：", dest)
	}
}

//因为要多次检查错误，所以建立一个函数。
func check(err error) {
	if err != nil {
		logger.Debug(err)
	}
}

//初始化全局变量
func readConfigFile(dir string) {
	logger.Debug("开始读取配置文件....")
	cfg, err := goconfig.LoadConfigFile(dir + "/config.ini")
	if err != nil {
		logger.Debug("无法加载配置文件：%s", err)
	}
	DBConn, _ = cfg.GetValue("Mysql", "DBConn")
	MaxConn, _ = cfg.Int("Mysql", "MaxConn")
	DestRoot, _ = cfg.GetValue("General", "DestRoot")
	DestCode, _ = cfg.Int("General", "DestCode")
	MaxThreads, _ = cfg.Int("General", "MaxThreads")
	MaxTasks, _ = cfg.Int("General", "MaxTasks")

	logger.Debug("DBConn: ", DBConn)
	logger.Debug("MaxConn: ", MaxConn)
	logger.Debug("DestRoot: ", DestRoot)
	logger.Debug("DestCode: ", DestCode)
	logger.Debug("MaxThreads: ", MaxThreads)
	logger.Debug("MaxTasks: ", MaxTasks)
}

func getFilePath(instancekey int64) (string, string) {
	logger.Debug("开始获取文件路径通过key:", instancekey)
	logger.Debug("***************************数据库空闲连接数：", db.Stats())
	sql := "SELECT i.file_name,m.img_file_name FROM instance i LEFT JOIN image m ON i.instance_key=m.instance_key WHERE i.instance_key=?;"
	var dicomPath string
	var jpgPath string
	row := db.QueryRow(sql, instancekey)
	// 1.获取dicom 和jpg 路径
	err := row.Scan(&dicomPath, &jpgPath)
	check(err)
	logger.Debug(instancekey, "获取到的文件名是：", dicomPath)
	logger.Debug(instancekey, "获取到的文件名是：", jpgPath)
	return dicomPath, jpgPath
}

func getinstancekey() {
	sql := `Select ins.instance_key from instance ins 
	where ins.location_code = ? and ins.FileExist!=2 
	order by instance_key ASC limit ?;`
	logger.Info("***************************数据库空闲连接数：", db.Stats())
	rows, err := db.Query(sql, DestCode, MaxTasks)
	if err != nil {
		logger.Fatal(err)
		return
	} else {
		for rows.Next() {
			var instance_key int64
			err = rows.Scan(&instance_key)
			check(err)
			logger.Debug("获取到的instance_key是： ", instance_key)
			dataChan <- instance_key
		}
		rows.Close()
	}
}

func updateFileFlag(instancekey int64) {
	logger.Debug("开始更新intance新存储位置：", instancekey)
	sql := "UPDATE instance set FileExist=2 where instance_key= ?;"
	db.Exec(sql, instancekey)
}

func initDB() {
	db, _ = sql.Open("mysql", DBConn)
	// 数据库最大连接数
	db.SetMaxOpenConns(MaxConn)
	//db.SetMaxIdleConns(MaxTasks)
	err := db.Ping()
	if err != nil {
		panic(err.Error())
	}
	logger.Debug("数据库连接成功...")
}

func Exist(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}
