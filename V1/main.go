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

//任务
type Job interface {
	// do something...
	Do()
}

//worker 工人
type Worker struct {
	JobQueue chan Job  //任务队列
	Quit     chan bool //停止当前任务
}

//新建一个 worker 通道实例  新建一个工人
func NewWorker() Worker {
	return Worker{
		JobQueue: make(chan Job), //初始化工作队列为null
		Quit:     make(chan bool),
	}
}

/*
整个过程中 每个Worker(工人)都会被运行在一个协程中，
在整个WorkerPool(领导)中就会有num个可空闲的Worker(工人)，
当来一条数据的时候，领导就会小组中取一个空闲的Worker(工人)去执行该Job，
当工作池中没有可用的worker(工人)时，就会阻塞等待一个空闲的worker(工人)。
每读到一个通道参数 运行一个 worker
*/

func (w Worker) Run(wq chan chan Job) {
	//这是一个独立的协程 循环读取通道内的数据，
	//保证 每读到一个通道参数就 去做这件事，没读到就阻塞
	go func() {
		for {
			wq <- w.JobQueue //注册工作通道  到 线程池
			select {
			case job := <-w.JobQueue: //读到参数
				job.Do()
			case <-w.Quit: //终止当前任务
				return
			}
		}
	}()
}

//workerpool 领导
type WorkerPool struct {
	workerlen   int      //线程池中  worker(工人) 的数量
	JobQueue    chan Job //线程池的  job 通道
	WorkerQueue chan chan Job
}

func NewWorkerPool(workerlen int) *WorkerPool {
	return &WorkerPool{
		workerlen:   workerlen,                      //开始建立 workerlen 个worker(工人)协程
		JobQueue:    make(chan Job),                 //工作队列 通道
		WorkerQueue: make(chan chan Job, workerlen), //最大通道参数设为 最大协程数 workerlen 工人的数量最大值
	}
}

//运行线程池
func (wp *WorkerPool) Run() {
	//初始化时会按照传入的num，启动num个后台协程，然后循环读取Job通道里面的数据，
	//读到一个数据时，再获取一个可用的Worker，并将Job对象传递到该Worker的chan通道
	logger.Debug("初始化worker")
	for i := 0; i < wp.workerlen; i++ {
		//新建 workerlen worker(工人) 协程(并发执行)，每个协程可处理一个请求
		worker := NewWorker() //运行一个协程 将线程池 通道的参数  传递到 worker协程的通道中 进而处理这个请求
		worker.Run(wp.WorkerQueue)
	}

	// 循环获取可用的worker,往worker中写job
	go func() { //这是一个单独的协程 只负责保证 不断获取可用的worker
		for {
			select {
			case job := <-wp.JobQueue: //读取任务
				//尝试获取一个可用的worker作业通道。
				//这将阻塞，直到一个worker空闲
				worker := <-wp.WorkerQueue
				worker <- job //将任务 分配给该工人
			}
		}
	}()
}

//----------------------------------------------
type Dosomething struct {
	key int64
}

func (d *Dosomething) Do() {
	// 获取dicom和JPG文件路径
	dicom, jpg := getFilePath(d.key)
	if dicom != "" {
		dicomdestpath := DestRoot + dicom
		logger.Debug(d.key, ": dicom目标文件路径： ", dicomdestpath)
		deleteFile(dicomdestpath)
	}
	if jpg != "" {
		jpgdestpath := DestRoot + jpg
		logger.Debug(d.key, ": jpg目标文件路径： ", jpgdestpath)
		deleteFile(jpgdestpath)
	}
	updateFileFlag(d.key)
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
	// 注册工作池，传入任务
	// 参数1 初始化worker(工人)设置最大线程数
	wokerPool := NewWorkerPool(MaxThreads)
	wokerPool.Run() //有任务就去做，没有就阻塞，任务做不过来也阻塞
	// 处理任务：
	go func() { //这是一个独立的协程 保证可以接受到每个用户的请求
		for {
			select {
			case instance_key := <-dataChan:
				sc := &Dosomething{key: instance_key}
				wokerPool.JobQueue <- sc //往线程池 的通道中 写参数   每个参数相当于一个请求  来了100万个请求
			}
		}
	}()
	// 主程序逻辑
	for {
		if db.Ping() != nil {
			initDB()
		}
		// 1.获取instance_key
		getinstancekey()
	}
}

func deleteFile(dest string) {
	if !Exist(dest) {
		logger.Debug(dest, "文件不存在.....")
		return
	} else {
		os.Remove(dest)
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
	logger.Debug("开始更新intance文件不存在标志：", instancekey)
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
