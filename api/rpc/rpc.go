package rpc

/**
 * Created by lock
 * Date: 2019-10-06
 * Time: 22:46
 */

import (
	"context"
	"github.com/rpcxio/libkv/store"
	etcdV3 "github.com/rpcxio/rpcx-etcd/client"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/client"
	"gochat/config"
	"gochat/proto"
	"sync"
	"time"
)

var LogicRpcClient client.XClient
var once sync.Once

type LogicRpc struct {
}

var LogicObjRpc *LogicRpc

func InitLogicRpcClient() {
	once.Do(func() {
		etcdConfigOption := &store.Config{
			ClientTLS:         nil,
			TLS:               nil,
			ConnectionTimeout: time.Duration(config.Conf.Common.CommonEtcd.ConnectionTimeout) * time.Second,
			Bucket:            "",
			PersistConnection: true,
			Username:          config.Conf.Common.CommonEtcd.UserName,
			Password:          config.Conf.Common.CommonEtcd.Password,
		}
		d, err := etcdV3.NewEtcdV3Discovery(
			config.Conf.Common.CommonEtcd.BasePath,
			config.Conf.Common.CommonEtcd.ServerPathLogic,
			[]string{config.Conf.Common.CommonEtcd.Host},
			true,
			etcdConfigOption,
		)
		if err != nil {
			logrus.Fatalf("init connect rpc etcd discovery client fail:%s", err.Error())
		}
		// 按照一定规则从etcd注册中心选出一个服务器
		LogicRpcClient = client.NewXClient(config.Conf.Common.CommonEtcd.ServerPathLogic, client.Failtry, client.RandomSelect, d, client.DefaultOption)
		LogicObjRpc = new(LogicRpc)
	})
	if LogicRpcClient == nil {
		logrus.Fatalf("get logic rpc client nil")
	}
}

func (rpc *LogicRpc) Login(req *proto.LoginRequest) (code int, authToken string, msg string) {
	reply := &proto.LoginResponse{}
	err := LogicRpcClient.Call(context.Background(), "Login", req, reply)
	if err != nil {
		msg = err.Error()
	}
	code = reply.Code
	authToken = reply.AuthToken
	return
}

func (rpc *LogicRpc) Register(req *proto.RegisterRequest) (code int, authToken string, msg string) {
	reply := &proto.RegisterReply{}
	err := LogicRpcClient.Call(context.Background(), "Register", req, reply)
	if err != nil {
		msg = err.Error()
	}
	code = reply.Code
	authToken = reply.AuthToken
	return
}

func (rpc *LogicRpc) GetUserNameByUserId(req *proto.GetUserInfoRequest) (code int, userName string) {
	reply := &proto.GetUserInfoResponse{}
	err := LogicRpcClient.Call(context.Background(), "GetUserInfoByUserId", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	userName = reply.UserName
	return
}

func (rpc *LogicRpc) CheckAuth(req *proto.CheckAuthRequest) (code int, userId int, userName string) {
	reply := &proto.CheckAuthResponse{}
	err := LogicRpcClient.Call(context.Background(), "CheckAuth", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	userId = reply.UserId
	userName = reply.UserName
	return
}

func (rpc *LogicRpc) Logout(req *proto.LogoutRequest) (code int) {
	reply := &proto.LogoutResponse{}
	err := LogicRpcClient.Call(context.Background(), "Logout", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	return
}

func (rpc *LogicRpc) Push(req *proto.Send) (code int, msg string) {
	reply := &proto.SuccessReply{}
	err := LogicRpcClient.Call(context.Background(), "Push", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	msg = reply.Msg
	return
}

func (rpc *LogicRpc) PushRoom(req *proto.Send) (code int, msg string) {
	reply := &proto.SuccessReply{}
	err := LogicRpcClient.Call(context.Background(), "PushRoom", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	msg = reply.Msg
	return
}

func (rpc *LogicRpc) Count(req *proto.Send) (code int, msg string) {
	reply := &proto.SuccessReply{}
	err := LogicRpcClient.Call(context.Background(), "Count", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	msg = reply.Msg
	return
}

func (rpc *LogicRpc) GetRoomInfo(req *proto.Send) (code int, msg string) {
	reply := &proto.SuccessReply{}
	err := LogicRpcClient.Call(context.Background(), "GetRoomInfo", req, reply)
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	code = reply.Code
	msg = reply.Msg
	return
}
