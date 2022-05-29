package task

/**
 * Created by lock
 * Date: 2019-08-13
 * Time: 10:13
 */
import (
	"context"
	"encoding/json"
	"errors"
	"github.com/rpcxio/libkv/store"
	etcdV3 "github.com/rpcxio/rpcx-etcd/client"
	"github.com/sirupsen/logrus"
	"github.com/smallnest/rpcx/client"
	"gochat/config"
	"gochat/proto"
	"gochat/tools"
	"strings"
	"sync"
	"time"
)

var RClient = &RpcConnectClient{
	ServerInsMap: make(map[string][]Instance),
	IndexMap:     make(map[string]int),
}

type Instance struct {
	ServerType string
	ServerId   string
	Client     client.XClient
}

type RpcConnectClient struct {
	lock         sync.Mutex
	ServerInsMap map[string][]Instance //serverId--[]ins
	IndexMap     map[string]int        //serverId--index
}

func (rc *RpcConnectClient) GetRpcClientByServerId(serverId string) (c client.XClient, err error) {
	rc.lock.Lock()
	defer rc.lock.Unlock()
	if _, ok := rc.ServerInsMap[serverId]; !ok || len(rc.ServerInsMap[serverId]) <= 0 {
		return nil, errors.New("no connect layer ip:" + serverId)
	}
	if _, ok := rc.IndexMap[serverId]; !ok {
		rc.IndexMap = map[string]int{
			serverId: 0,
		}
	}
	idx := rc.IndexMap[serverId] % len(rc.ServerInsMap[serverId])
	ins := rc.ServerInsMap[serverId][idx]
	rc.IndexMap[serverId] = (rc.IndexMap[serverId] + 1) % len(rc.ServerInsMap[serverId])
	return ins.Client, nil
}

func (rc *RpcConnectClient) GetAllConnectTypeRpcClient() (rpcClientList []client.XClient) {
	for serverId := range rc.ServerInsMap {
		c, err := rc.GetRpcClientByServerId(serverId)
		if err != nil {
			logrus.Infof("GetAllConnectTypeRpcClient err:%s", err.Error())
			continue
		}
		rpcClientList = append(rpcClientList, c)
	}
	return
}

func getParamByKey(s string, key string) string {
	params := strings.Split(s, "&")
	for _, p := range params {
		kv := strings.Split(p, "=")
		if len(kv) == 2 && kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

func (task *Task) InitConnectRpcClient() (err error) {
	etcdConfigOption := &store.Config{
		ClientTLS:         nil,
		TLS:               nil,
		ConnectionTimeout: time.Duration(config.Conf.Common.CommonEtcd.ConnectionTimeout) * time.Second, // etcd超时项目默认配置为5
		Bucket:            "",
		PersistConnection: true,
		Username:          config.Conf.Common.CommonEtcd.UserName,
		Password:          config.Conf.Common.CommonEtcd.Password,
	}
	etcdConfig := config.Conf.Common.CommonEtcd
	d, e := etcdV3.NewEtcdV3Discovery(
		etcdConfig.BasePath,
		etcdConfig.ServerPathConnect,
		[]string{etcdConfig.Host},
		true,
		etcdConfigOption,
	)
	if e != nil {
		logrus.Fatalf("init task rpc etcd discovery client fail:%s", e.Error())
	}
	// 获取所有注册到etcd上的服务
	if len(d.GetServices()) <= 0 {
		logrus.Panicf("no etcd server find!")
	}
	for _, connectConf := range d.GetServices() {
		logrus.Infof("key is:%s,value is:%s", connectConf.Key, connectConf.Value)
		//RpcConnectClients
		serverType := getParamByKey(connectConf.Value, "serverType")
		serverId := getParamByKey(connectConf.Value, "serverId")
		logrus.Infof("serverType is:%s,serverId is:%s", serverType, serverId)
		if serverType == "" || serverId == "" {
			continue
		}
		// 从etcd上获取到服务后，客户端和对应服务进行点对点通信
		d, e := client.NewPeer2PeerDiscovery(connectConf.Key, "")
		if e != nil {
			logrus.Errorf("init task client.NewPeer2PeerDiscovery client fail:%s", e.Error())
			continue
		}
		c := client.NewXClient(etcdConfig.ServerPathConnect, client.Failtry, client.RandomSelect, d, client.DefaultOption)
		ins := Instance{
			ServerType: serverType,
			ServerId:   serverId,
			Client:     c,
		}
		if _, ok := RClient.ServerInsMap[serverId]; !ok {
			RClient.ServerInsMap[serverId] = []Instance{ins}
		} else {
			RClient.ServerInsMap[serverId] = append(RClient.ServerInsMap[serverId], ins)
		}
	}
	// watch connect server change && update RpcConnectClientList
	// TODO: 监听服务的状态改变情况
	go task.watchServicesChange(d)
	return
}

func (task *Task) watchServicesChange(d client.ServiceDiscovery) {
	etcdConfig := config.Conf.Common.CommonEtcd
	for kvChan := range d.WatchService() {
		// 无可用服务的情况
		if len(kvChan) <= 0 {
			logrus.Errorf("connect services change, connect alarm, no abailable ip")
		}
		logrus.Infof("connect services change trigger...")
		insMap := make(map[string][]Instance)
		for _, kv := range kvChan {
			logrus.Infof("connect services change,key is:%s,value is:%s", kv.Key, kv.Value)

			serverType := getParamByKey(kv.Value, "serverType")
			serverId := getParamByKey(kv.Value, "serverId")
			logrus.Infof("serverType is:%s,serverId is:%s", serverType, serverId)
			if serverType == "" || serverId == "" {
				continue
			}
			d, e := client.NewPeer2PeerDiscovery(kv.Key, "")
			if e != nil {
				logrus.Errorf("init task client.NewPeer2PeerDiscovery watch client fail:%s", e.Error())
				continue
			}
        // older--version
		// 	syncLock.Lock() //如果把锁加到for语句下面，则会阻塞所有没有发生异常的服务
		// 	// 随机获取connect层中可用的服务
		// 	RpcConnectClientList[serverId] = client.NewXClient(etcdConfig.ServerPathConnect, client.Failtry, client.RandomSelect, d, client.DefaultOption)
		// 	syncLock.Unlock()
		// }
		// syncLock.Lock()
		// for oldServerId := range RpcConnectClientList {
		// 	if _, ok := newServerIdMap[oldServerId]; !ok {
		// 		// 从服务列表中删除已经失效的服务
		// 		delete(RpcConnectClientList, oldServerId)
			c := client.NewXClient(etcdConfig.ServerPathConnect, client.Failtry, client.RandomSelect, d, client.DefaultOption)
			ins := Instance{
				ServerType: serverType,
				ServerId:   serverId,
				Client:     c,
			}
			if _, ok := insMap[serverId]; !ok {
				insMap[serverId] = []Instance{ins}
			} else {
				insMap[serverId] = append(insMap[serverId], ins)
			}
		}
		RClient.lock.Lock()
		RClient.ServerInsMap = insMap
		RClient.lock.Unlock()

	}
}

func (task *Task) pushSingleToConnect(serverId string, userId int, msg []byte) {
	logrus.Infof("pushSingleToConnect Body %s", string(msg))
	pushMsgReq := &proto.PushMsgRequest{
		UserId: userId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpSingleSend,
			SeqId:     tools.GetSnowflakeId(), // SeqId 利用雪花算法产生不重复的id
			Body:      msg,
		},
	}
	reply := &proto.SuccessReply{}
// <<<<<<< HEAD old version
// 	//todo lock，这里需要上读锁吗？在这里上锁的话如果watchServicesChange监听到服务状态发生改变？
// 	//从根据serverId选择对应的服务，后调用对应的rpc服务
// 	// TODO: 这里使用的serverId很可能是已经失效了的服务，（此处调用的是Connect层提供的rpc服务），
// 	//因为检测服务时，如果服务改变

// 	// fix bug:
// 	if xxc, ok := RpcConnectClientList[serverId]; !ok {
// 		// 如果对应connect的服务已经下线了，则把数据重新换一个可用的serverId写入redis中，
// 		// TODO：为什么要引入serverId机制？？？，直接用能用的服务不行？？？
// 		fmt.Println(xxc)
// 	}

// 	// 这里对应serverId 很可能不存在了（因为失效而被删除了）
// 	err := RpcConnectClientList[serverId].Call(context.Background(), "PushSingleMsg", pushMsgReq, reply)
// =======
	connectRpc, err := RClient.GetRpcClientByServerId(serverId)
	if err != nil {
		logrus.Infof("get rpc client err %v", err)
	}
	err = connectRpc.Call(context.Background(), "PushSingleMsg", pushMsgReq, reply)
	if err != nil {
		logrus.Infof("pushSingleToConnect Call err %v", err)
	}
	logrus.Infof("reply %s", reply.Msg)
}

func (task *Task) broadcastRoomToConnect(roomId int, msg []byte) {
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomSend,
			SeqId:     tools.GetSnowflakeId(),
			Body:      msg,
		},
	}
	reply := &proto.SuccessReply{}
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomToConnect rpc  %v", rpc)
		rpc.Call(context.Background(), "PushRoomMsg", pushRoomMsgReq, reply)
		logrus.Infof("reply %s", reply.Msg)
	}
}

func (task *Task) broadcastRoomCountToConnect(roomId, count int) {
	msg := &proto.RedisRoomCountMsg{
		Count: count,
		Op:    config.OpRoomCountSend,
	}
	var body []byte
	var err error
	if body, err = json.Marshal(msg); err != nil {
		logrus.Warnf("broadcastRoomCountToConnect  json.Marshal err :%s", err.Error())
		return
	}
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomCountSend,
			SeqId:     tools.GetSnowflakeId(),
			Body:      body,
		},
	}
	reply := &proto.SuccessReply{}
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomCountToConnect rpc  %v", rpc)
		rpc.Call(context.Background(), "PushRoomCount", pushRoomMsgReq, reply)
		logrus.Infof("reply %s", reply.Msg)
	}
}

func (task *Task) broadcastRoomInfoToConnect(roomId int, roomUserInfo map[string]string) {
	msg := &proto.RedisRoomInfo{
		Count:        len(roomUserInfo),
		Op:           config.OpRoomInfoSend,
		RoomUserInfo: roomUserInfo,
		RoomId:       roomId,
	}
	var body []byte
	var err error
	if body, err = json.Marshal(msg); err != nil {
		logrus.Warnf("broadcastRoomInfoToConnect  json.Marshal err :%s", err.Error())
		return
	}
	pushRoomMsgReq := &proto.PushRoomMsgRequest{
		RoomId: roomId,
		Msg: proto.Msg{
			Ver:       config.MsgVersion,
			Operation: config.OpRoomInfoSend,
			SeqId:     tools.GetSnowflakeId(),
			Body:      body,
		},
	}
	reply := &proto.SuccessReply{}
	rpcList := RClient.GetAllConnectTypeRpcClient()
	for _, rpc := range rpcList {
		logrus.Infof("broadcastRoomInfoToConnect rpc  %v", rpc)
		rpc.Call(context.Background(), "PushRoomInfo", pushRoomMsgReq, reply)
		logrus.Infof("broadcastRoomInfoToConnect rpc  reply %v", reply)
	}
}
