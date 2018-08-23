<?php
/**
 * Created by PhpStorm.
 * User: Xia Zheng Xin
 * Date: 2018/8/23
 * Time: 下午6:56
 */

use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\ConsumerTopic;
use RdKafka\Producer;
use RdKafka\ProducerTopic;
use RdKafka\TopicConf;

/**
 * Class MQ
 * @package App\Utility
 * @see https://arnaud-lb.github.io/php-rdkafka/phpdoc/book.rdkafka.html
 */
class MQ
{
    /**
     * @var ConsumerTopic
     */
    private $consumerTopic;

    private $consumerTopicPart;

    /**
     * @var ProducerTopic
     */
    private $producerTopic;

    /**
     * MQ constructor.
     * @param ConsumerTopic $consumerTopic
     * @param ProducerTopic $producerTopic
     */
    public function __construct(ConsumerTopic $consumerTopic = null, ProducerTopic $producerTopic = null)
    {
        $this->consumerTopic = $consumerTopic;
        $this->producerTopic = $producerTopic;
    }

    function pullUnreadFromIt(callable $func, $part = 0, $timeout = 10000000)
    {
        if ($this->consumerTopic) {
            $this->consumerTopic->consumeStart($part, RD_KAFKA_OFFSET_STORED);
            $this->consumerTopicPart = $part;
            while (true) {
                $msg = $this->consumerTopic->consume($part, $timeout);
                if (is_null($msg)) {
                    $func(null, "ERROR");
                    break;
                }
                if ($msg->err != RD_KAFKA_RESP_ERR_NO_ERROR && $msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    $func(null, $msg->errstr());
                    break;
                } else {
                    $func($msg, null);
                    $this->consumerTopic->offsetStore($part, $msg->offset);
                }
            }
        }
    }

    function pullAllFromIt(callable $func, $part = 0, $timeout = 10000000)
    {
        if ($this->consumerTopic) {
            $this->consumerTopic->consumeStart($part, RD_KAFKA_OFFSET_BEGINNING);
            $this->consumerTopicPart = $part;
            while (true) {
                $msg = $this->consumerTopic->consume($part, $timeout);
                if (is_null($msg)) {
                    $func(null, "ERROR");
                    break;
                }
                if ($msg->err != RD_KAFKA_RESP_ERR_NO_ERROR && $msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    $func(null, $msg->errstr());
                    break;
                } else {
                    $func($msg, null);
                    $this->consumerTopic->offsetStore($part, $msg->offset);
                }
            }
        }
    }

    function pullNewFromIt(callable $func, $part = 0, $timeout = 10000000)
    {
        if ($this->consumerTopic) {
            $this->consumerTopic->consumeStart($part, RD_KAFKA_OFFSET_END);
            $this->consumerTopicPart = $part;
            while (true) {
                $msg = $this->consumerTopic->consume($part, $timeout);
                if (is_null($msg)) {
                    $func(null, "ERROR");
                    break;
                }
                if ($msg->err != RD_KAFKA_RESP_ERR_NO_ERROR && $msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    $func(null, $msg->errstr());
                    break;
                } else {
                    $func($msg, null);
                    $this->consumerTopic->offsetStore($part, $msg->offset);
                }
            }
        }
    }

    function stopPull()
    {
        if ($this->consumerTopic) {
            $this->consumerTopic->consumeStop($this->consumerTopicPart);
        }
    }

    /**
     * @param string $host
     * @param int $level
     * @return Producer
     */
    static function getProducer($host = "127.0.0.1", $level = LOG_DEBUG): Producer
    {
        $rk = new Producer();
        $rk->setLogLevel($level);
        $rk->addBrokers($host);
        return $rk;
    }

    /**
     * @param string $host
     * @param int $level
     * @param string $topic
     * @param TopicConf $topicConf
     * @return ProducerTopic
     */
    static function getProducerTopic($host = "127.0.0.1", $level = LOG_DEBUG, $topic = "", $topicConf = null): ProducerTopic
    {
        $rk = new Producer();
        $rk->setLogLevel($level);
        $rk->addBrokers($host);
        return $rk->newTopic($topic, $topicConf);
    }

    /**
     * @param $config Conf
     * @return Producer
     */
    static function getProducerByConfig($config): Producer
    {
        return new Producer($config);
    }

    /**
     * @param string $host
     * @param int $level
     * @return Consumer
     */
    static function getConsumer($host = "127.0.0.1", $level = LOG_DEBUG): Consumer
    {
        $rk = new Consumer();
        $rk->setLogLevel($level);
        $rk->addBrokers($host);
        return $rk;
    }

    /**
     * @param string $host
     * @param int $level
     * @param string $topic
     * @param TopicConf $topicConf
     * @return ConsumerTopic
     */
    static function getConsumerTopic($host = "127.0.0.1", $level = LOG_DEBUG, $topic = "", $topicConf = null): ConsumerTopic
    {
        $rk = new Consumer();
        $rk->setLogLevel($level);
        $rk->addBrokers($host);
        return $rk->newTopic($topic, $topicConf);
    }

    /**
     * @param $config Conf
     * @return Consumer
     */
    static function getConsumerByConfig($config): Consumer
    {
        return new Consumer($config);
    }
}