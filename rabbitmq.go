package rabbitmq

import (
	"ethsync/common/log"
	"fmt"
)

//type AddrInfo struct {
//	AddressInfo  bo.UserAddressInfo `json:"addressInfo"`
//	ContractInfo po.ContractInfo    `json:"contractInfo"`
//}

type ReceiveFun func(data []byte, header map[string]interface{}, retryClient RetryClientInterface) bool

func NewRabbitMq(hostPort, username, password string) *RabbitMq {

	instanceConsumePool := NewConsumePool()
	//instanceConsumePool.SetMaxConsumeChannel(100)
	err := instanceConsumePool.Connect("amqps", hostPort, username, password)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	return &RabbitMq{
		Pool: instanceConsumePool,
	}
}

type RabbitMq struct {
	Pool *RabbitPool
}

func (mq *RabbitMq) Consume(queueName string, receiveFun ReceiveFun) {

	nomrl := &ConsumeReceive{
		// 定义消费者事件
		ExchangeName: queueName, //队列名称
		ExchangeType: EXCHANGE_TYPE_FANOUT,
		Route:        queueName,
		QueueName:    queueName,
		IsTry:        true, //是否重试
		MaxReTry:     5,    //最大重试次数
		EventFail: func(code int, e error, data []byte) {
			fmt.Printf("error:%s", e)
		},

		/***
		 * 参数说明
		 * @param data []byte 接收的rabbitmq数据
		 * @param header map[string]interface{} 原rabbitmq header
		 * @param retryClient Rabbitrabbitmq.RetryClientInterface 自定义重试数据接口，重试需return true 防止数据重复提交
		 ***/
		EventSuccess: receiveFun,
	}

	mq.Pool.RegisterConsumeReceive(nomrl)
	err := mq.Pool.RunConsume()
	if err != nil {
		log.Error(err)
	}
}
