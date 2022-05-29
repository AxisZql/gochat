package task

/**
 * Created by lock
 * Date: 2019-08-13
 * Time: 10:13
 */
import (
	"github.com/go-redis/redis"
	"github.com/sirupsen/logrus"
	"gochat/config"
	"gochat/tools"
	"time"
)

var RedisClient *redis.Client

func (task *Task) InitQueueRedisClient() (err error) {
	redisOpt := tools.RedisOption{
		Address:  config.Conf.Common.CommonRedis.RedisAddress,
		Password: config.Conf.Common.CommonRedis.RedisPassword,
		Db:       config.Conf.Common.CommonRedis.Db,
	}
	RedisClient = tools.GetRedisInstance(redisOpt)
	if pong, err := RedisClient.Ping().Result(); err != nil {
		logrus.Infof("RedisClient Ping Result pong: %s,  err: %s", pong, err)
	}
	go func() {
		for {
			var result []string
			//10s timeout
			// TODO: Redis BRPop 命令移出并获取列表的最后一个元素， 如果列表没有元素会阻塞列表直到等待超时或发现可弹出元素为止。
			// TODO: 由于redis的这种特性，故达到消息队列的效果
			// 如果超过10秒没有获取消息就会输出错误日志
			result, err = RedisClient.BRPop(time.Second*10, config.QueueName).Result()
			if err != nil {
				logrus.Infof("task queue block timeout,no msg err:%s", err.Error())
			}
			// 将从redis模拟的消息队列中获取出来的消息通过task层推送给connect进而推送到对应客户端
			// 由于获取结果是包含消息的key 和value 的string slice 故len(result)==2,
			//TODO：> 2的情况是什么（logic层推送消息巨多造成异常？）
			if len(result) >= 2 {
				// key对应的value推送给connect层，进而推送到客户端
				task.Push(result[1])
			}
		}
	}()
	return
}
