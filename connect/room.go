package connect

/**
 * Created by lock
 * Date: 2019-08-09
 * Time: 15:18
 */
import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gochat/proto"
	"sync"
)

const NoRoom = -1

type Room struct {
	Id          int
	OnlineCount int // room online user count
	rLock       sync.RWMutex
	drop        bool // make room is live
	next        *Channel
}

func NewRoom(roomId int) *Room {
	room := new(Room)
	room.Id = roomId
	room.drop = false
	room.next = nil
	room.OnlineCount = 0
	return room
}

// Put 用户加入房间聊天会话
func (r *Room) Put(ch *Channel) (err error) {
	//doubly linked list,采用前插链表的方式来新增Channel（会话）节点
	r.rLock.Lock()
	defer r.rLock.Unlock()
	// 要判断当前聊天室是否在线
	if !r.drop {
		if r.next != nil {
			r.next.Prev = ch
		}
		ch.Next = r.next
		ch.Prev = nil
		r.next = ch
		r.OnlineCount++
	} else {
		err = errors.New("room drop")
	}
	return
}

// Push 往房间中所有在线用户发送消息
func (r *Room) Push(msg *proto.Msg) {
	r.rLock.RLock()
	for ch := r.next; ch != nil; ch = ch.Next {
		if err := ch.Push(msg); err != nil {
			logrus.Infof("push msg err:%s", err.Error())
		}
	}
	r.rLock.RUnlock()
	return
}

func (r *Room) DeleteChannel(ch *Channel) bool {
	r.rLock.RLock()
	if ch.Next != nil {
		//if not footer
		ch.Next.Prev = ch.Prev
	}
	if ch.Prev != nil {
		// if not header
		ch.Prev.Next = ch.Next
	} else {
		r.next = ch.Next
	}
	r.OnlineCount--
	r.drop = false
	// TODO:房间在线用户数为0时，房间会失效
	if r.OnlineCount <= 0 {
		r.drop = true
	}
	r.rLock.RUnlock()
	return r.drop
}
