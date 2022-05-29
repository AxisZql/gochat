package api

/**
 * Created by lock
 * Date: 2019-08-12
 * Time: 11:17
 */
import (
	"context"
	"flag"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	"gochat/api/router"
	"gochat/api/rpc"
	"gochat/config"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Chat struct {
}

func New() *Chat {
	return &Chat{}
}

// Run api server,Also, you can use gin,echo ... framework wrap
func (c *Chat) Run() {
	//init rpc client
	rpc.InitLogicRpcClient()

	// 注册路由和中间件
	r := router.Register()
	runMode := config.GetGinRunMode()
	logrus.Info("server start , now run mode is ", runMode)
	gin.SetMode(runMode)
	// 项目默认配置的api端口为7070
	apiConfig := config.Conf.Api
	port := apiConfig.ApiBase.ListenPort
	flag.Parse()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("start listen : %s\n", err)
		}
	}()
	// if have two quit signal , this signal will priority capture ,also can graceful shutdown
	// 信号捕获，优雅退出
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	<-quit
	logrus.Infof("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logrus.Errorf("Server Shutdown:", err)
	}
	logrus.Infof("Server exiting")
	os.Exit(0)
}
