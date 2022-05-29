package task

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 18:22
 * Desc: task层主要是用来消费redis消息队列中已经生产出来的消息
 */
import (
	"github.com/sirupsen/logrus"
	"gochat/config"
	"runtime"
)

type Task struct {
}

func New() *Task {
	return new(Task)
}

func (task *Task) Run() {
	//read config
	taskConfig := config.Conf.Task
	// 项目默认配置最多使用的cpu核心数为4
	runtime.GOMAXPROCS(taskConfig.TaskBase.CpuNum)
	//read from redis queue
	if err := task.InitQueueRedisClient(); err != nil {
		logrus.Panicf("task init publishRedisClient fail,err:%s", err.Error())
	}
	//rpc call connect layer send msg
	// 从etcd中获取所有注册的服务列表，并新建goroutine来监听etcd上注册的服务的变化
	if err := task.InitConnectRpcClient(); err != nil {
		logrus.Panicf("task init InitConnectRpcClient fail,err:%s", err.Error())
	}
	//GoPush
	task.GoPush()
}
