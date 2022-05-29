package connect

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 18:18
 */
import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gochat/config"
	_ "net/http/pprof"
	"runtime"
	"time"
)

var DefaultServer *Server

type Connect struct {
	ServerId string
}

func New() *Connect {
	return new(Connect)
}

func (c *Connect) Run() {
	// get Connect layer config
	connectConfig := config.Conf.Connect

	//set the maximum number of CPUs that can be executing
	// 设置CPU的最大可以执行个数
	runtime.GOMAXPROCS(connectConfig.ConnectBucket.CpuNum)

	//init logic layer rpc client, call logic layer rpc server
	//初始化rpc逻辑层客户端，调用rpc逻辑层服务端
	if err := c.InitLogicRpcClient(); err != nil {
		logrus.Panicf("InitLogicRpcClient err:%s", err.Error())
	}
	//init Connect layer rpc server, logic client will call this
	//初始化连接层RPC服务器，逻辑客户端将调用此,设定多少核cpu那就初始化多少个桶（bucket）
	Buckets := make([]*Bucket, connectConfig.ConnectBucket.CpuNum)
	for i := 0; i < connectConfig.ConnectBucket.CpuNum; i++ {
		Buckets[i] = NewBucket(BucketOptions{
			ChannelSize:   connectConfig.ConnectBucket.Channel,       // TODO:一个桶(或者一个房间)可以处理的会话数目，项目配置是1024
			RoomSize:      connectConfig.ConnectBucket.Room,          // 一个桶可以管理的房间数目，项目配置是1024
			RoutineAmount: connectConfig.ConnectBucket.RoutineAmount, // 队列中的routine(请求)缓冲区数目，项目配置是32个
			RoutineSize:   connectConfig.ConnectBucket.RoutineSize,   // 队列中每个请求缓冲区包含的请求数目，项目配置的是20个
		})
	}
	// operator 主要负责调用logic层的Connect和DisConnect方法
	operator := new(DefaultOperator)
	DefaultServer = NewServer(Buckets, operator, ServerOptions{
		WriteWait:       10 * time.Second,
		PongWait:        60 * time.Second,
		PingPeriod:      54 * time.Second,
		MaxMessageSize:  512,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		BroadcastSize:   512,
	})
	// uuid生成的唯一的serverId
	c.ServerId = fmt.Sprintf("%s-%s", "ws", uuid.New().String())
	//init Connect layer rpc server ,task layer will call this
	// 初始化WebSocket服务调用logic层rpc服务
	if err := c.InitConnectWebsocketRpcServer(); err != nil {
		logrus.Panicf("InitConnectWebsocketRpcServer Fatal error: %s \n", err.Error())
	}

	//start Connect layer server handler persistent connection
	// 启动对外暴露WebSocket服务的RESTful Api，使用golang的net/http库实现
	if err := c.InitWebsocket(); err != nil {
		logrus.Panicf("Connect layer InitWebsocket() error:  %s \n", err.Error())
	}
}

func (c *Connect) RunTcp() {
	// get Connect layer config
	connectConfig := config.Conf.Connect

	//set the maximum number of CPUs that can be executing
	runtime.GOMAXPROCS(connectConfig.ConnectBucket.CpuNum)

	//init logic layer rpc client, call logic layer rpc server
	if err := c.InitLogicRpcClient(); err != nil {
		logrus.Panicf("InitLogicRpcClient err:%s", err.Error())
	}
	//init Connect layer rpc server, logic client will call this
	Buckets := make([]*Bucket, connectConfig.ConnectBucket.CpuNum)
	for i := 0; i < connectConfig.ConnectBucket.CpuNum; i++ {
		Buckets[i] = NewBucket(BucketOptions{
			ChannelSize:   connectConfig.ConnectBucket.Channel,
			RoomSize:      connectConfig.ConnectBucket.Room,
			RoutineAmount: connectConfig.ConnectBucket.RoutineAmount,
			RoutineSize:   connectConfig.ConnectBucket.RoutineSize,
		})
	}
	operator := new(DefaultOperator)
	DefaultServer = NewServer(Buckets, operator, ServerOptions{
		WriteWait:       10 * time.Second,
		PongWait:        60 * time.Second,
		PingPeriod:      54 * time.Second,
		MaxMessageSize:  512,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		BroadcastSize:   512,
	})
	//go func() {
	//	http.ListenAndServe("0.0.0.0:9000", nil)
	//}()
	// 一个ServerId持有多个Bucket，而一个Bucket持有多个room，一个room持有多个channel
	c.ServerId = fmt.Sprintf("%s-%s", "tcp", uuid.New().String())
	//init Connect layer rpc server ,task layer will call this
	if err := c.InitConnectTcpRpcServer(); err != nil {
		logrus.Panicf("InitConnectWebsocketRpcServer Fatal error: %s \n", err.Error())
	}
	//start Connect layer server handler persistent connection by tcp
	// 通过TCP启动连接层服务器处理程序持久连接
	if err := c.InitTcpServer(); err != nil {
		logrus.Panicf("Connect layerInitTcpServer() error:%s\n ", err.Error())
	}
}
