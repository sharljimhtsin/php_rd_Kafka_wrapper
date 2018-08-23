# 很方便的RD_Kafka php 操作包装类

```
//通过静态方法获取 消费者主题
$topic = MQ::getConsumer()->newTopic("test");
//使用消费者主题实例 填充生成 MQ包装类 对象
$mq = new MQ($topic);
//开始轮询RdKafka 主题内的消息队列
$mq->pullAllFromIt(function ($msg, $err) {
    var_dump($msg);// \RdKafka\Message 对象
    var_dump($err);// 错误信息
}, 0, 60 * 60 * 24 * 30 * 1000);
//停止轮询
$mq->stopPull();
```

依赖:

librdkafka: https://github.com/edenhill/librdkafka (系统动态链接库文件)

php-rdkafka: https://github.com/arnaud-lb/php-rdkafka (PHP 扩展DLL/SO)

Stubs for PHP Rdkafka extension: 
https://github.com/kwn/php-rdkafka-stubs (Composer 安装)
