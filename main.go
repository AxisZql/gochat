package main

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 10:56
 */

import (
	"flag"
	"fmt"
	"gochat/api"
	"gochat/connect"
	"gochat/logic"
	"gochat/site"
	"gochat/task"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	var module string
	// 利用命令行标志来设置运行模式
	flag.StringVar(&module, "module", "", "assign run module")
	flag.Parse()
	fmt.Println(fmt.Sprintf("start run %s module", module))
	// 不同的module对应一个微服务（即每个module对应一个独立的服务）
	switch module {
	case "logic":
		// rpc方式的服务
		logic.New().Run()
	case "connect_websocket":
		// websocket 方式的服务
		connect.New().Run()
	case "connect_tcp":
		connect.New().RunTcp()
	case "task":
		task.New().Run()
	case "api":
		api.New().Run()
	case "site":
		// 前端站点
		site.New().Run()
	default:
		fmt.Println("exiting,module param error!")
		return
	}
	fmt.Println(fmt.Sprintf("run %s module done!", module))
	quit := make(chan os.Signal)
	// 收到系统的中断信号会往quit channel种写入信息然后退出（优雅退出）
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
	fmt.Println("Server exiting")
}
