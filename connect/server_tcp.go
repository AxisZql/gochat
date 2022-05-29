package connect

/**
 * Created by lock
 * Date: 2020/4/14
 */
import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"gochat/api/rpc"
	"gochat/config"
	"gochat/pkg/stickpackage"
	"gochat/proto"
	"net"
	"strings"
	"time"
)

// TODO：为什么使用无符号整数？（防止出现负数故设置最大值为2^31-1)
const maxInt = 1<<31 - 1

func init() {
	rpc.InitLogicRpcClient()
}

func (c *Connect) InitTcpServer() error {
	// tcp connect 服务启动在7001和7002端口上
	aTcpAddr := strings.Split(config.Conf.Connect.ConnectTcp.Bind, ",")
	// 项目配置的默认cpu核数是4
	cpuNum := config.Conf.Connect.ConnectBucket.CpuNum
	var (
		addr     *net.TCPAddr
		listener *net.TCPListener
		err      error
	)
	for _, ipPort := range aTcpAddr {
		// 将字符串解析*net.TcpAddr
		if addr, err = net.ResolveTCPAddr("tcp", ipPort); err != nil {
			logrus.Errorf("server_tcp ResolveTCPAddr error:%s", err.Error())
			return err
		}
		if listener, err = net.ListenTCP("tcp", addr); err != nil {
			logrus.Errorf("net.ListenTCP(tcp, %s),error(%v)", ipPort, err)
			return err
		}
		logrus.Infof("start tcp listen at:%s", ipPort)
		// cpu core num
		for i := 0; i < cpuNum; i++ {
			// 由于项目配置的默认cpu核心数是4，故创建4个goroutine来监听客户端请求
			go c.acceptTcp(listener)
		}
	}
	return nil
}

func (c *Connect) acceptTcp(listener *net.TCPListener) {
	var (
		conn *net.TCPConn
		err  error
		r    int
	)
	connectTcpConfig := config.Conf.Connect.ConnectTcp
	for {
		if conn, err = listener.AcceptTCP(); err != nil {
			logrus.Errorf("listener.Accept(\"%s\") error(%v)", listener.Addr().String(), err)
			return
		}
		// set keep alive，client==server ping package check
		// 如果设置keep alive （长连接）则必须在客户端和服务端利用心跳包进行心跳检测
		// 本项目中配置中keep alive的选项为false
		if err = conn.SetKeepAlive(connectTcpConfig.KeepAlive); err != nil {
			logrus.Errorf("conn.SetKeepAlive() error:%s", err.Error())
			return
		}
		//set ReceiveBuf
		// 本项目中配置ReceiveBuf为4096 --> 4k
		if err := conn.SetReadBuffer(connectTcpConfig.ReceiveBuf); err != nil {
			logrus.Errorf("conn.SetReadBuffer() error:%s", err.Error())
			return
		}
		//set SendBuf
		// 本项目中配置SendBuf为4096 --> 4k
		if err := conn.SetWriteBuffer(connectTcpConfig.SendBuf); err != nil {
			logrus.Errorf("conn.SetWriteBuffer() error:%s", err.Error())
			return
		}
		// 监听到客户端请求后，开辟一个新的goroutine去处理对应的请求，防止阻塞而导致新的连接建立请求无法及时处理
		go c.ServeTcp(DefaultServer, conn, r)
		if r++; r == maxInt {
			logrus.Infof("conn.acceptTcp num is:%d", r)
			r = 0
		}
	}
}

func (c *Connect) ServeTcp(server *Server, conn *net.TCPConn, r int) {
	var ch *Channel
	// 初始化Channel（会话）
	ch = NewChannel(server.Options.BroadcastSize)
	ch.connTcp = conn
	go c.writeDataToTcp(server, ch)
	go c.readDataFromTcp(server, ch)
}

// readDataFromTcp 获取客户端通过tcp发送过来消息，并进行相应的拆包，消息推送处理
func (c *Connect) readDataFromTcp(s *Server, ch *Channel) {
	defer func() {
		logrus.Infof("start exec disConnect ...")
		if ch.Room == nil || ch.userId == 0 {
			logrus.Infof("roomId and userId eq 0")
			_ = ch.connTcp.Close()
			return
		}
		logrus.Infof("exec disConnect ...")
		disConnectRequest := new(proto.DisConnectRequest)
		disConnectRequest.RoomId = ch.Room.Id
		disConnectRequest.UserId = ch.userId
		// TODO: 每个服务实例在建立之初都初始化了令牌桶Bucket
		s.Bucket(ch.userId).DeleteChannel(ch)
		if err := s.operator.DisConnect(disConnectRequest); err != nil {
			logrus.Warnf("DisConnect rpc err :%s", err.Error())
		}
		if err := ch.connTcp.Close(); err != nil {
			logrus.Warnf("DisConnect close tcp conn err :%s", err.Error())
		}
		return
	}()
	// scanner
	scannerPackage := bufio.NewScanner(ch.connTcp)
	// 防止粘包，进行包的拆解
	scannerPackage.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		// TODO: 这里为什么要data[0]=='v'(是因为 VersionContent [2]byte = [2]byte{'v', '1'}）
		if !atEOF && data[0] == 'v' {
			//stickpackage.TcpHeaderLength 之所以等于4，这是因为version和length字段分别占用来2字节
			if len(data) > stickpackage.TcpHeaderLength {
				packSumLength := int16(0)
				// 拆解出当前数据包的Length信息
				_ = binary.Read(bytes.NewReader(data[stickpackage.LengthStartIndex:stickpackage.LengthStopIndex]), binary.BigEndian, &packSumLength)
				// 如果data整体部分的数据包长度大于当前数据包的Length，则证明发生了粘包，故需要进行拆包
				if int(packSumLength) <= len(data) {
					// data[:packSumLength]就是拆解出来的一个数据包
					return int(packSumLength), data[:packSumLength], nil
				}
			}
		}
		return
	})
	scanTimes := 0
	for {
		scanTimes++
		if scanTimes > 3 {
			logrus.Infof("scannedPack times is:%d", scanTimes)
			break
		}
		// 由于上面已经进行数据包的拆包了，故每次Scan得到的都是独立的一个数据包
		for scannerPackage.Scan() {
			scannedPack := new(stickpackage.StickPackage)
			// 把数据包中的对应数据读取出来
			err := scannedPack.Unpack(bytes.NewReader(scannerPackage.Bytes()))
			if err != nil {
				logrus.Errorf("scan tcp package err:%s", err.Error())
				break
			}
			//get a full package
			var connReq proto.ConnectRequest
			logrus.Infof("get a tcp message :%s", scannedPack)
			var rawTcpMsg proto.SendTcp
			// 对当前package中的Msg进行反序列化，提取出消息内容
			if err := json.Unmarshal(scannedPack.Msg, &rawTcpMsg); err != nil {
				logrus.Errorf("tcp message struct %+v", rawTcpMsg)
				break
			}
			logrus.Infof("json unmarshal,raw tcp msg is:%+v", rawTcpMsg)
			if rawTcpMsg.AuthToken == "" {
				logrus.Errorf("tcp s.operator.Connect no authToken")
				return
			}
			if rawTcpMsg.RoomId <= 0 {
				logrus.Errorf("tcp roomId not allow lgt 0")
				return
			}
			switch rawTcpMsg.Op {
			// 建立tcp连接
			case config.OpBuildTcpConn:
				connReq.AuthToken = rawTcpMsg.AuthToken
				connReq.RoomId = rawTcpMsg.RoomId
				//fix
				//connReq.ServerId = config.Conf.Connect.ConnectTcp.ServerId
				connReq.ServerId = c.ServerId
				userId, err := s.operator.Connect(&connReq)
				logrus.Infof("tcp s.operator.Connect userId is :%d", userId)
				if err != nil {
					logrus.Errorf("tcp s.operator.Connect error %s", err.Error())
					return
				}
				if userId == 0 {
					logrus.Error("tcp Invalid AuthToken ,userId empty")
					return
				}
				b := s.Bucket(userId)
				//insert into a bucket
				err = b.Put(userId, connReq.RoomId, ch)
				if err != nil {
					logrus.Errorf("tcp conn put room err: %s", err.Error())
					_ = ch.connTcp.Close()
					return
				}
			// 发送tcp消息给房间
			case config.OpRoomSend:
				//send tcp msg to room
				req := &proto.Send{
					Msg:          rawTcpMsg.Msg,
					FromUserId:   rawTcpMsg.FromUserId,
					FromUserName: rawTcpMsg.FromUserName,
					RoomId:       rawTcpMsg.RoomId,
					Op:           config.OpRoomSend,
				}
				// 通过api层调用logic层的rpc服务，把消息发送到消息队列中
				code, msg := rpc.LogicObjRpc.PushRoom(req)
				logrus.Infof("tcp conn push msg to room,err code is:%d,err msg is:%s", code, msg)
			}
		}
		if err := scannerPackage.Err(); err != nil {
			logrus.Errorf("tcp get a err package:%s", err.Error())
			return
		}
	}
}

func (c *Connect) writeDataToTcp(s *Server, ch *Channel) {
	//ping time default 54s，心跳检测周期项目配置默认为54s
	ticker := time.NewTicker(DefaultServer.Options.PingPeriod)
	defer func() {
		ticker.Stop()
		_ = ch.connTcp.Close()
		return
	}()
	// 设置数据包的版本信息
	pack := stickpackage.StickPackage{
		Version: stickpackage.VersionContent,
	}
	for {
		select {
		case message, ok := <-ch.broadcast:
			if !ok {
				_ = ch.connTcp.Close()
				return
			}
			pack.Msg = message.Body
			// 数据包长度为头部4字节加上消息体长度
			pack.Length = pack.GetPackageLength()
			//send msg
			logrus.Infof("send tcp msg to conn:%s", pack.String())
			// 消息通过io.Writer往连接通道中写入
			if err := pack.Pack(ch.connTcp); err != nil {
				logrus.Errorf("connTcp.write message err:%s", err.Error())
				return
			}
		case <-ticker.C:
			logrus.Infof("connTcp.ping message,send")
			//send a ping msg ,if error , return
			pack.Msg = []byte("ping msg")
			pack.Length = pack.GetPackageLength()
			// 发送心跳检测包
			if err := pack.Pack(ch.connTcp); err != nil {
				//send ping msg to tcp conn
				return
			}
		}
	}
}
