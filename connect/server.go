package connect

/**
 * Created by lock
 * Date: 2019-08-10
 * Time: 18:32
 */
import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket" // websocket第三方库
	"github.com/sirupsen/logrus"   // 日志库
	"gochat/proto"
	"gochat/tools"
	"time"
)

type Server struct {
	Buckets   []*Bucket
	Options   ServerOptions
	bucketIdx uint32 //记录令牌桶的个数
	operator  Operator
}

type ServerOptions struct {
	WriteWait       time.Duration
	PongWait        time.Duration
	PingPeriod      time.Duration
	MaxMessageSize  int64
	ReadBufferSize  int
	WriteBufferSize int
	BroadcastSize   int
}

func NewServer(b []*Bucket, o Operator, options ServerOptions) *Server {
	s := new(Server)
	s.Buckets = b // 一台服务器上的Server持有所有本服务器上产生的令牌桶
	s.Options = options
	s.bucketIdx = uint32(len(b))
	s.operator = o
	return s
}

// Bucket reduce lock competition, use Google city hash insert to different bucket
func (s *Server) Bucket(userId int) *Bucket {
	userIdStr := fmt.Sprintf("%d", userId)
	idx := tools.CityHash32([]byte(userIdStr), uint32(len(userIdStr))) % s.bucketIdx
	return s.Buckets[idx]
}

func (s *Server) writePump(ch *Channel, c *Connect) {
	// TODO： 如果客户端异常而退出，本项目这里怎么实时刷新在线人数
	//PingPeriod default eq 54s,TODO: 心跳检测周期默认设置为54s
	ticker := time.NewTicker(s.Options.PingPeriod)
	defer func() {
		ticker.Stop()
		err := ch.conn.Close()
		if err != nil {
			logrus.Errorf("%+v", err)
		}
	}()

	for {
		select {
		// 从广播channel中获取出广播消息
		case message, ok := <-ch.broadcast:
			//write data dead time , like http timeout , default 10s
			// 设置websocket发送消息最长容忍时间
			err := ch.conn.SetWriteDeadline(time.Now().Add(s.Options.WriteWait))
			if err != nil {
				logrus.Warn(" ch.conn.SetWriteDeadline err :%s  ", err.Error())
			}
			if !ok {
				logrus.Warn("SetWriteDeadline not ok")
				//如果channel中是0值则发送空消息
				err = ch.conn.WriteMessage(websocket.CloseMessage, []byte{})
				if err != nil {
					logrus.Warn(" ch.conn.WriteMessage err :%s  ", err.Error())
				}
				return
			}
			// 设置消息数据的格式
			w, err := ch.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				logrus.Warn(" ch.conn.NextWriter err :%s  ", err.Error())
				return
			}
			logrus.Infof("message write body:%s", message.Body)
			_, err = w.Write(message.Body)
			if err != nil {
				logrus.Warn(" w.Write err :%s  ", err.Error())
			}
			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			// TODO：触发心跳检测
			//heartbeat，if ping error will exit and close current websocket conn
			err := ch.conn.SetWriteDeadline(time.Now().Add(s.Options.WriteWait))
			if err != nil {
				logrus.Warn(" ch.conn.SetWriteDeadline err :%s  ", err.Error())
			}
			logrus.Infof("websocket.PingMessage :%v", websocket.PingMessage)
			if err := ch.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (s *Server) readPump(ch *Channel, c *Connect) {
	defer func() {
		logrus.Infof("start exec disConnect ...")
		// TODO：简单用户下线ch.Room==nil ？ 简单房间下线ch.userId == 0 ?
		if ch.Room == nil || ch.userId == 0 {
			logrus.Infof("roomId and userId eq 0")
			err := ch.conn.Close()
			if err != nil {
				logrus.Warn(" ch.conn.Close err :%s  ", err.Error())
			}
			return
		}
		logrus.Infof("exec disConnect ...")
		disConnectRequest := new(proto.DisConnectRequest)
		disConnectRequest.RoomId = ch.Room.Id
		disConnectRequest.UserId = ch.userId
		// 从桶中删除对应用户或者房间的数据，以此表示对应用户、房间已经下线
		s.Bucket(ch.userId).DeleteChannel(ch)
		// 调用logic层rpc服务来下线对应房间和用户
		if err := s.operator.DisConnect(disConnectRequest); err != nil {
			logrus.Warnf("DisConnect err :%s", err.Error())
		}
		err := ch.conn.Close()
		if err != nil {
			logrus.Warn("  ch.conn.Close err :%s  ", err.Error())
		}
	}()

	// MaxMessageSize 默认消息大小最大为512byte
	ch.conn.SetReadLimit(s.Options.MaxMessageSize)
	// 默认设置读取消息最长超时时间为60s
	err := ch.conn.SetReadDeadline(time.Now().Add(s.Options.PongWait))
	if err != nil {
		logrus.Warn("  ch.conn.SetReadDeadline :%s  ", err.Error())
	}
	// 设置心跳包回复处理函数
	ch.conn.SetPongHandler(func(string) error {
		// 设置心跳包读取最长超时时间
		err = ch.conn.SetReadDeadline(time.Now().Add(s.Options.PongWait))
		if err != nil {
			logrus.Warn("  ch.conn.SetReadDeadline :%s  ", err.Error())
		}
		return nil
	})

	for {
		_, message, err := ch.conn.ReadMessage()
		if err != nil {
			// 判断连接关闭类型是否websocket.CloseGoingAway、websocket.CloseAbnormalClosure（异常关闭）其中之一
			// 如果不是则返回true，如果是则返回false
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				logrus.Errorf("readPump ReadMessage err:%s", err.Error())
				return
			}
		}
		if message == nil {
			return
		}
		var connReq *proto.ConnectRequest
		logrus.Infof("get a message :%s", message)
		// 发连接请求
		if err := json.Unmarshal(message, &connReq); err != nil {
			logrus.Errorf("message struct %+v", connReq)
		}
		if connReq.AuthToken == "" {
			logrus.Errorf("s.operator.Connect no authToken")
			return
		}
		connReq.ServerId = c.ServerId //config.Conf.Connect.ConnectWebsocket.ServerId
		// 调用logic层连接的rpc服务
		userId, err := s.operator.Connect(connReq)
		if err != nil {
			logrus.Errorf("s.operator.Connect error %s", err.Error())
			return
		}
		if userId == 0 {
			logrus.Error("Invalid AuthToken ,userId empty")
			return
		}
		logrus.Infof("websocket rpc call return userId:%d,RoomId:%d", userId, connReq.RoomId)
		// 获取桶
		b := s.Bucket(userId)
		//insert into a bucket
		err = b.Put(userId, connReq.RoomId, ch)
		if err != nil {
			logrus.Errorf("conn close err: %s", err.Error())
			err = ch.conn.Close()
			if err != nil {
				logrus.Warn("  ch.conn.Close :%s  ", err.Error())
			}
		}
	}
}
