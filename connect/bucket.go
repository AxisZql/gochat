package connect

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 15:18
 */
import (
	"gochat/proto"
	"sync"
	"sync/atomic"
)

type Bucket struct {
	cLock         sync.RWMutex     // protect the channels for chs
	chs           map[int]*Channel // map sub key to a channel
	bucketOptions BucketOptions
	rooms         map[int]*Room // bucket room channels
	routines      []chan *proto.PushRoomMsgRequest
	routinesNum   uint64
	broadcast     chan []byte
}

type BucketOptions struct {
	ChannelSize   int
	RoomSize      int
	RoutineAmount uint64
	RoutineSize   int
}

func NewBucket(bucketOptions BucketOptions) (b *Bucket) {
	b = new(Bucket)
	b.chs = make(map[int]*Channel, bucketOptions.ChannelSize)
	b.bucketOptions = bucketOptions
	// 为slice分配空间
	b.routines = make([]chan *proto.PushRoomMsgRequest, bucketOptions.RoutineAmount)
	b.rooms = make(map[int]*Room, bucketOptions.RoomSize)
	for i := uint64(0); i < b.bucketOptions.RoutineAmount; i++ {
		// 为slice中每个channel分配空间
		c := make(chan *proto.PushRoomMsgRequest, bucketOptions.RoutineSize)
		b.routines[i] = c
		go b.PushRoom(c)
	}
	return
}

func (b *Bucket) PushRoom(ch chan *proto.PushRoomMsgRequest) {
	for {
		var (
			arg  *proto.PushRoomMsgRequest
			room *Room
		)
		arg = <-ch
		if room = b.Room(arg.RoomId); room != nil {
			room.Push(&arg.Msg)
		}
	}
}

func (b *Bucket) Room(rid int) (room *Room) {
	b.cLock.RLock()
	room, _ = b.rooms[rid]
	b.cLock.RUnlock()
	return
}

// Put 给桶中加入新的房间会话实例
func (b *Bucket) Put(userId int, roomId int, ch *Channel) (err error) {
	var (
		room *Room
		ok   bool
	)
	b.cLock.Lock()
	if roomId != NoRoom {
		// 没有房间则创建新的房间
		if room, ok = b.rooms[roomId]; !ok {
			room = NewRoom(roomId)
			b.rooms[roomId] = room
		}
		ch.Room = room
	}
	ch.userId = userId
	b.chs[userId] = ch
	b.cLock.Unlock()

	// 将新的会话加入房间中
	if room != nil {
		err = room.Put(ch)
	}
	return
}

// DeleteChannel 删除对应对用户会话
func (b *Bucket) DeleteChannel(ch *Channel) {
	var (
		ok   bool
		room *Room
	)
	// TODO：这里涉及对数据对写操作，为什么加读锁，而不是写锁
	// 是否因为多个Channel（用户会话）是相互独立对原因，写锁对话，其他用户就不能进行
	// 任何状态修改操作
	b.cLock.RLock()
	if ch, ok = b.chs[ch.userId]; ok {
		room = b.chs[ch.userId].Room
		//delete from bucket
		delete(b.chs, ch.userId)
	}
	// 如果私聊房间已经没有人，则删除在桶中删除当前房间（表示当前房间下线）
	if room != nil && room.DeleteChannel(ch) {
		// if room empty delete,will mark room.drop is true
		if room.drop == true {
			delete(b.rooms, room.Id)
		}
	}
	b.cLock.RUnlock()
}

func (b *Bucket) Channel(userId int) (ch *Channel) {
	b.cLock.RLock()
	ch = b.chs[userId]
	b.cLock.RUnlock()
	return
}

func (b *Bucket) BroadcastRoom(pushRoomMsgReq *proto.PushRoomMsgRequest) {
	//TODO：原子加法,记录所有请求数，bucketOptions.RoutineAmount 限定了系统最多同时RoutineAmount个请求
	num := atomic.AddUint64(&b.routinesNum, 1) % b.bucketOptions.RoutineAmount
	b.routines[num] <- pushRoomMsgReq
}
