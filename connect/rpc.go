package connect

/**
 * Created by lock
 * Date: 2019-08-12
 * Time: 23:36
 * Desc: connect层 rpc服务的定义和注册
 */
import (
	"context"
	"errors"
	"fmt"
	"gochat/config"
	"gochat/proto"
	"gochat/tools"
	"strings"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"
	"github.com/rpcxio/libkv/store"
	etcdV3 "github.com/rpcxio/rpcx-etcd/client"
	"github.com/rpcxio/rpcx-etcd/serverplugin"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/client"
	"github.com/smallnest/rpcx/server"
)

var logicRpcClient client.XClient
var once sync.Once

type RpcConnect struct {
}

// InitLogicRpcClient 利用etcd服务发现来初始化调用rpc到客户端
func (c *Connect) InitLogicRpcClient() (err error) {
	// 连接到etcd服务需要进行到配置
	etcdConfigOption := &store.Config{
		ClientTLS:         nil,
		TLS:               nil,
		ConnectionTimeout: time.Duration(config.Conf.Common.CommonEtcd.ConnectionTimeout) * time.Second,
		Bucket:            "",
		PersistConnection: true,
		Username:          config.Conf.Common.CommonEtcd.UserName,
		Password:          config.Conf.Common.CommonEtcd.Password,
	}
	once.Do(func() {
		d, e := etcdV3.NewEtcdV3Discovery(
			config.Conf.Common.CommonEtcd.BasePath,
			config.Conf.Common.CommonEtcd.ServerPathLogic,
			[]string{config.Conf.Common.CommonEtcd.Host},
			true,
			etcdConfigOption,
		)
		if e != nil {
			logrus.Fatalf("init connect rpc etcd discovery client fail:%s", e.Error())
		}
		// 构造获取logic层rpc服务的客户端
		logicRpcClient = client.NewXClient(config.Conf.Common.CommonEtcd.ServerPathLogic, client.Failtry, client.RandomSelect, d, client.DefaultOption)
	})
	if logicRpcClient == nil {
		return errors.New("get rpc client nil")
	}
	return
}

// Connect 初始化聊天室上线连接
func (rpc *RpcConnect) Connect(connReq *proto.ConnectRequest) (uid int, err error) {
	reply := &proto.ConnectReply{}
	// 调用远程rpc方法--connect
	err = logicRpcClient.Call(context.Background(), "Connect", connReq, reply)
	if err != nil {
		logrus.Fatalf("failed to call: %v", err)
	}
	uid = reply.UserId
	logrus.Infof("connect logic userId :%d", reply.UserId)
	return
}

func (rpc *RpcConnect) DisConnect(disConnReq *proto.DisConnectRequest) (err error) {
	reply := &proto.DisConnectReply{}
	/// 调用离线rpc方法
	if err = logicRpcClient.Call(context.Background(), "DisConnect", disConnReq, reply); err != nil {
		logrus.Fatalf("failed to call: %v", err)
	}
	return
}

// InitConnectWebsocketRpcServer  初始化化WebSocketRpc服务
func (c *Connect) InitConnectWebsocketRpcServer() (err error) {
	var network, addr string
	//[connect-rpcAddress-websocket]
	//address = "tcp@0.0.0.0:6912,tcp@0.0.0.0:6913"---> webSocket在6912和6913上起服务
	connectRpcAddress := strings.Split(config.Conf.Connect.ConnectRpcAddressWebSockets.Address, ",")
	for _, bind := range connectRpcAddress {
		// tools.ParseNetwork 就是简单的字符串分隔函数--> tcp@0.0.0.0:6912 --> tcp,0.0.0.0:6912
		if network, addr, err = tools.ParseNetwork(bind); err != nil {
			logrus.Panicf("InitConnectWebsocketRpcServer ParseNetwork error : %s", err)
		}
		logrus.Infof("Connect start run at-->%s:%s", network, addr)
		go c.createConnectWebsocketsRpcServer(network, addr)
	}
	return
}

func (c *Connect) InitConnectTcpRpcServer() (err error) {
	var network, addr string
	// 开启connect层的rpc服务在6912和6913端口上
	connectRpcAddress := strings.Split(config.Conf.Connect.ConnectRpcAddressTcp.Address, ",")
	for _, bind := range connectRpcAddress {
		if network, addr, err = tools.ParseNetwork(bind); err != nil {
			logrus.Panicf("InitConnectTcpRpcServer ParseNetwork error : %s", err)
		}
		logrus.Infof("Connect start run at-->%s:%s", network, addr)
		go c.createConnectTcpRpcServer(network, addr)
	}
	return
}

// RpcConnectPush 定义Connection的rpc服务
type RpcConnectPush struct{}

// PushSingleMsg 私聊消息
func (rpc *RpcConnectPush) PushSingleMsg(ctx context.Context, pushMsgReq *proto.PushMsgRequest, successReply *proto.SuccessReply) (err error) {
	var (
		bucket  *Bucket
		channel *Channel
	)
	logrus.Info("rpc PushMsg :%v ", pushMsgReq)
	if pushMsgReq == nil {
		logrus.Errorf("rpc PushSingleMsg() args:(%v)", pushMsgReq)
		return
	}
	// TODO：获取令牌桶
	bucket = DefaultServer.Bucket(pushMsgReq.UserId)
	// 获取令牌桶后，接着获取用户会话实例
	if channel = bucket.Channel(pushMsgReq.UserId); channel != nil {
		// 往广播channel中写入消息体中
		err = channel.Push(&pushMsgReq.Msg)
		// 如果对应用户接收消息的用户不在线则发送不了消息（TODO:缺乏离线消息机制是本项目的一个缺点）
		// TODO：有一个想法用于支持离线消息（如果使用Kafka的话则为每个发送方建立一个独立的topic）
		// TODO：私聊的话，两方都是消息的生产者，由服务端充当一个消费者
		// TODO：群聊的话，群中所有用户都是消息的生产者，由服务端充当一个消费者
		// 由于topic中的消息没有被消费则会一直存在，故达到离线消息的效果（接收方收到消息后会把消息持久化到数据库中）
		logrus.Infof("DefaultServer Channel err nil ,args: %v", pushMsgReq)
		return
	}
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("successReply:%v", successReply)
	return
}

// TODO：同样功能的函数为什么要分成PushRoomMsg、PushRoomCount、PushRoomInfo 三个函数写（为了调用时不混淆？）

// PushRoomMsg 群聊消息
func (rpc *RpcConnectPush) PushRoomMsg(ctx context.Context, pushRoomMsgReq *proto.PushRoomMsgRequest, successReply *proto.SuccessReply) (err error) {
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("PushRoomMsg msg %+v", pushRoomMsgReq)
	for _, bucket := range DefaultServer.Buckets {
		bucket.BroadcastRoom(pushRoomMsgReq)
	}
	return
}

func (rpc *RpcConnectPush) PushRoomCount(ctx context.Context, pushRoomMsgReq *proto.PushRoomMsgRequest, successReply *proto.SuccessReply) (err error) {
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("PushRoomCount msg %v", pushRoomMsgReq)
	for _, bucket := range DefaultServer.Buckets {
		bucket.BroadcastRoom(pushRoomMsgReq)
	}
	return
}

func (rpc *RpcConnectPush) PushRoomInfo(ctx context.Context, pushRoomMsgReq *proto.PushRoomMsgRequest, successReply *proto.SuccessReply) (err error) {
	successReply.Code = config.SuccessReplyCode
	successReply.Msg = config.SuccessReplyMsg
	logrus.Infof("connect,PushRoomInfo msg %+v", pushRoomMsgReq)
	for _, bucket := range DefaultServer.Buckets {
		bucket.BroadcastRoom(pushRoomMsgReq)
	}
	return
}

// TODO：WebSocket和TCP的Rpc服务功能是相同的，从本系统的架构图上可以看出

func (c *Connect) createConnectWebsocketsRpcServer(network string, addr string) {
	s := server.NewServer()
	// 添加etcd配置并连接etcd集群
	addRegistryPlugin(s, network, addr)
	//config.Conf.Connect.ConnectTcp.ServerId
	//s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("%s", config.Conf.Connect.ConnectWebsocket.ServerId))
	// <<<<<<< HEAD
	// 	err := s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("%s", c.ServerId))
	// 	if err != nil {
	// 		logrus.Errorf(fmt.Sprintf("%+v", err))
	// 	}
	// =======
	_ = s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("serverId=%s&serverType=ws", c.ServerId))
	s.RegisterOnShutdown(func(s *server.Server) {
		err := s.UnregisterAll()
		if err != nil {
			logrus.Errorf("%+v", err)
		}
	})
	err := s.Serve(network, addr)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
}

func (c *Connect) createConnectTcpRpcServer(network string, addr string) {
	s := server.NewServer()
	addRegistryPlugin(s, network, addr)
	//s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("%s", config.Conf.Connect.ConnectTcp.ServerId))
	// <<<<<<< HEAD
	// 	err := s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("%s", c.ServerId))
	// 	if err != nil {
	// 		logrus.Errorf(fmt.Sprintf("%+v", err))
	// 	}
	// =======
	_ = s.RegisterName(config.Conf.Common.CommonEtcd.ServerPathConnect, new(RpcConnectPush), fmt.Sprintf("serverId=%s&serverType=tcp", c.ServerId))
	s.RegisterOnShutdown(func(s *server.Server) {
		err := s.UnregisterAll()
		if err != nil {
			logrus.Errorf(fmt.Sprintf("%+v", err))
		}
	})
	err := s.Serve(network, addr)
	if err != nil {
		logrus.Errorf(fmt.Sprintf("%+v", err))
	}
}

// 添加etcd配置并连接etcd集群
func addRegistryPlugin(s *server.Server, network string, addr string) {
	r := &serverplugin.EtcdV3RegisterPlugin{
		// 本机监听地址
		ServiceAddress: network + "@" + addr,
		// etcd集群的地址
		EtcdServers: []string{config.Conf.Common.CommonEtcd.Host},
		// 服务前缀。如果有多个项目同时使用zookeeper，避免命名冲突，可以设置这个参数，为当前服务的设置命名空间
		BasePath: config.Conf.Common.CommonEtcd.BasePath,
		// TODO：·不清楚· 用来更新服务的TPS
		Metrics: metrics.NewRegistry(),
		// 服务的刷新间隔，如果在一定间隔内（当前设2*UpdateInterval）没有刷新，服务就会从etcd中删除
		UpdateInterval: time.Minute,
	}
	// 开始连接etcd集群
	err := r.Start()
	if err != nil {
		logrus.Fatal(err)
	}
	// 当前服务添加etcd配置
	s.Plugins.Add(r)
}
