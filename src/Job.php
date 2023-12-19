<?php

declare(strict_types=1);

namespace Limphp\Jobs;

use Throwable;
use Exception;

/**
 * Job类
 *
 */
abstract class Job
{
    /**
     * 主题，即队列键
     * @var string
     */
    public $topic = '';

    /**
     * 是否为延迟队列
     * @var boolean
     */
    public $isDelay = false;

    /**
     * 常驻静态子进程数，范围1-1000
     * @var integer
     */
    public $staticWorkerCount = 1;

    /**
     * 临时动态子进程数，范围1-1000
     * @var integer
     */
    public $dynamicWorkerCount = 4;

    /**
     * 健康的队列积压数，当队列积压过多时启用动态子进程，特殊的，0表示无视积压，即不启用动态子进程
     * @var integer
     */
    public $healthQueueLength = 100;

    /**
     * 单次子进程最长运行时间，单位秒
     * @var integer
     */
    public $maxExecuteTime = 100;

    /**
     * 单例
     * @var Job
     */
    private static $instance = null;

    /**
     * 运行时参数：子进程信息
     * @var array
     */
    public $workers = [];

    /**
     * 运行时参数：禁止创建子进程的截止时间，当子进程出错时为防止频繁创建进程，会限制创建一段时间
     * @var integer
     */
    public $workerEnabledTime = 0;

    /**
     * 队列实例
     * @var mixed
     */
    public $queue = null;

    /**
     * 获取队列配置
     * @return array
     */
    abstract public function getQueueConfig(): array;

    /**
     * 消费消息
     * @param string $message
     * @return void
     */
    abstract public function doJob(string $message): void;

    /**
     * 单例
     * @return static
     */
    public final static function instance()
    {
        if (!self::$instance) {
            self::$instance = new static();
        }
        return self::$instance;
    }

    /**
     * 投递消息，仅供业务端使用
     * @param string $message
     * @param integer $expectedRunTime 期望执行时间，仅延迟类型的队列有效
     * @return boolean
     */
    public static function deliver(string $message, int $expectedRunTime = 0): bool
    {
        $job = self::instance();
        if ($job->isDelay) {
            return $job->adapterCommandRedis(function () use ($job, $message, $expectedRunTime) {
                return $job->queue->zAdd($job->topic, $expectedRunTime, $message) ? true : false;
            });
        } else {
            return $job->adapterCommandRedis(function () use ($job, $message) {
                return $job->queue->lPush($job->topic, $message) ? true : false;
            });
        }
    }

    /**
     * 撤销延迟投递，仅供业务端使用
     * @param string $message
     * @return boolean
     */
    public static function revokeDelayDeliver(string $message): bool
    {
        $job = self::instance();
        if ($job->isDelay) {
            return $job->adapterCommandRedis(function () use ($job, $message) {
                return $job->queue->zRem(self::instance()->topic, $message) ? true : false;
            });
        }
        return false;
    }

    /**
     * 阻塞弹出一条消息，注意队列连接超时问题
     * @param integer $timeout
     * @return string|null
     */
    public function brPop(int $timeout = 1): ?string
    {
        $message = null;
        if ($this->isDelay) {
            $data = $this->adapterCommandRedis(function () {
                return $this->queue->zRangeByScore($this->topic, 0, time(), ['withscores' => false, 'limit' => [0, 1]]);
            });
            if (!!$data) {
                $message = $data[0];
                $this->adapterCommandRedis(function () use ($message) {
                    return $this->queue->zRem($this->topic, $message);
                });
            } else {
                usleep(1000000 * $timeout);
            }
        } else {
            $data = $this->adapterCommandRedis(function () use ($timeout) {
                return $this->queue->brPop($this->topic, $timeout);
            });
            if (!!$data) {
                $message = $data[1];
            }
        }

        return $message;
    }

    /**
     * 队列积压长度，注意队列连接超时问题
     * @param integer $timeout
     * @return integer
     */
    public function size(): int
    {
        if ($this->isDelay) {
            $data = $this->adapterCommandRedis(function () {
                return $this->queue->zCount($this->topic, 0, time());
            });
        } else {
            $data = $this->adapterCommandRedis(function () {
                return $this->queue->lLen($this->topic);
            });
        }

        return $data ? intval($data) : 0;
    }

    /**
     * 连接Redis
     */
    private final function adapterConnectRedis(): void
    {
        try {
            $config = $this->getQueueConfig();
            $this->queue = new \Redis();
            $this->queue->connect($config['host'], $config['port'], 3);
            $this->queue->select($config['database']);
            if (!empty($config['password'])) {
                $this->queue->auth($config['password']);
            }
        } catch (Throwable $e) {
            throw $e;
        }
    }

    /**
     * 执行Redis命令
     * @param callable $callback
     * @return mixed
     */
    public final function adapterCommandRedis(callable $callback)
    {
        try {
            if (!$this->queue) {
                $this->adapterConnectRedis();
            }
            return call_user_func($callback);
        } catch (\RedisException $e) {
            $this->adapterConnectRedis();
            if ($this->queue->isConntected()) {
                return call_user_func($callback);
            }
        }
        throw new Exception('redis connect error');
    }
}