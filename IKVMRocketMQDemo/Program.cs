using org.apache.rocketmq.client.consumer;
using org.apache.rocketmq.client.consumer.listener;
using org.apache.rocketmq.client.producer;
using org.apache.rocketmq.common.consumer;
using org.apache.rocketmq.common.message;
using System.Text;

namespace IKVMRocketMQDemo
{
    /// <summary>
    /// 消息队列使用示例
    /// 更多，请参照：http://rocketmq.apache.org/docs/schedule-example/
    /// </summary>
    internal class Program
    {
        private static void Main(string[] args)
        {
            #region 生产消息
            DefaultMQProducer producer = null;
            try
            {
                ////wulangtes  是分组名字，用来区分生产者
                producer = new DefaultMQProducer("wulangtes");

                ////服务器ip
                producer.setNamesrvAddr("192.168.10.2:9876");
                producer.start();

                ////Go_Ticket_WuLang_Test 为 toptic 名字，taga是比toptic更为精确地内容划分，RocketMQ会重试是内容
                Message msg = new Message("Go_Ticket_WuLang_Test", "TagA", Encoding.UTF8.GetBytes("RocketMQ会重试 "));
                SendResult sendResult = producer.send(msg);
            }
            finally
            {
                producer.shutdown();
            }
            #endregion

            #region 消费者
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test");
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
            consumer.setNamesrvAddr("192.168.10.2:9876");
            ////Go_Ticket_WuLang_Test toptic名字，* 表示不过滤tag，如果过滤，可使用 ||划分
            consumer.subscribe("Go_Ticket_WuLang_Test", "*");
            consumer.registerMessageListener(new TestListener());
            consumer.start();
            #endregion
        }

        public class TestListener : MessageListenerConcurrently
        {
            /// <summary>
            /// 消费功能，消费失败返回ConsumeConcurrentlyStatus.RECONSUME_LATER，成功返回CONSUME_SUCCESS
            /// </summary>
            /// <param name="list">虽然是个list，但是list的size是一个</param>
            /// <param name="ccc">上下文，对一些参数做设置，例如</param>
            /// <returns>结果</returns>
            public ConsumeConcurrentlyStatus consumeMessage(java.util.List list, ConsumeConcurrentlyContext ccc)
            {
                for (int i = 0; i < list.size(); i++)
                {
                    var msg = list.get(i) as Message;
                    byte[] body = msg.getBody();
                    var str = Encoding.UTF8.GetString(body);
                    if (body.Length == 2 && body[0] == 0 && body[1] == 0)
                    {

                        //System.out.println("Got the end signal");
                        continue;
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }

        }
    }
}
