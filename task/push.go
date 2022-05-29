package task

/**
 * Created by lock
 * Date: 2019-08-13
 * Time: 10:50
 */

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"gochat/config"
	"gochat/proto"
	"math/rand"
)

type PushParams struct {
	ServerId string
	UserId   int
	Msg      []byte
	RoomId   int
}

var pushChannel []chan *PushParams

func init() {
	// 项目配置的PushChan大小默认为2
	pushChannel = make([]chan *PushParams, config.Conf.Task.TaskBase.PushChan)
}

func (task *Task) GoPush() {
	for i := 0; i < len(pushChannel); i++ {
		// 项目配置的pushChanSize大小默认为50
		pushChannel[i] = make(chan *PushParams, config.Conf.Task.TaskBase.PushChanSize)
		go task.processSinglePush(pushChannel[i])
	}
}

func (task *Task) processSinglePush(ch chan *PushParams) {
	var arg *PushParams
	for {
		arg = <-ch
		//@todo when arg.ServerId server is down, user could be reconnected other serverId but msg in queue no consume
		//难点（亮点）： 如果当前ServeId的服务崩溃来，用户可以重新连接其他ServeId的服务，但是在队列中的消息没有被成功消费（从而造成信息丢失）
		task.pushSingleToConnect(arg.ServerId, arg.UserId, arg.Msg)
	}
}

// Push 将从redis中获取出来的消息推送到connect层
func (task *Task) Push(msg string) {
	m := &proto.RedisMsg{}
	if err := json.Unmarshal([]byte(msg), m); err != nil {
		logrus.Infof(" json.Unmarshal err:%v ", err)
	}
	logrus.Infof("push msg info %v", m)
	switch m.Op {
	case config.OpSingleSend:
		pushChannel[rand.Int()%config.Conf.Task.TaskBase.PushChan] <- &PushParams{
			ServerId: m.ServerId, // 由于serverId是产生该消息的服务，如果该服务崩溃了，这条消息就丢失了（没有被消费）
			UserId:   m.UserId,
			Msg:      m.Msg,
		}
	case config.OpRoomSend:
		task.broadcastRoomToConnect(m.RoomId, m.Msg)
	case config.OpRoomCountSend:
		task.broadcastRoomCountToConnect(m.RoomId, m.Count)
	case config.OpRoomInfoSend:
		task.broadcastRoomInfoToConnect(m.RoomId, m.RoomUserInfo)
	}
}
