<?php

declare(strict_types=1);

namespace Limphp\Jobs;

use Exception;
use Throwable;

/**
 * 多进程Job任务管理器
 * 
 */
class Process
{
    /**
     * 主进程文件
     * @var string
     */
    private $masterPidFile = '';

    /**
     * 日志文件
     * @var string
     */
    private $logFile = '';

    /**
     * Job实例数组
     * @var Job
     */
    private $jobs = [];


    /**
     * 构造方法
     * @param string $runtimeDir 运行时存储进程文件和日志的目录
     */
    public function __construct(string $runtimeDir)
    {
        $this->masterPidFile = rtrim($runtimeDir, '/') . '/master.pid';
        $this->logFile = rtrim($runtimeDir, '/') . '/logs/process.log';

        set_exception_handler(function ($e) {
            $this->log($e->getMessage(), true);
        });

        set_error_handler(function (int $errorNo, string $errorMsg) {
            $this->log($errorMsg, true);
        });
    }

    /**
     * 初始化
     * @param string $runtimeDir
     * @return self
     */
    public static function init(string $runtimeDir): self
    {
        return new self($runtimeDir);
    }

    /**
     * 注册Job
     * @param Job $job
     * @return self
     */
    public function registerJob(Job $job): self
    {
        if (is_string($job->topic) && '' !== $job->topic) {
            $job->staticWorkerCount = min(1000, max(1, intval($job->staticWorkerCount)));
            $job->dynamicWorkerCount = min(1000, max(1, intval($job->dynamicWorkerCount)));
            $job->healthQueueLength = intval($job->healthQueueLength);
            $job->workers = [];
            $job->workerEnabledTime = 0;
            $this->jobs[$job->topic] = $job;
        }
        return $this;
    }

    /**
     * 执行脚本
     * @param string $cmd
     * @return void
     */
    public function execute(string $cmd): void
    {
        switch ($cmd) {
            case 'start':
                $this->start();
                break;
            case 'stop':
                $this->stop();
                break;
            case 'restart':
                if ($this->stop()) {
                    $this->start();
                }
                break;
            case 'status':
                if (!!$masterPid = $this->getMasterPid()) {
                    echo "process is running, pid={$masterPid}\n";
                } else {
                    echo "process is not running\n";
                }
                break;
            default:
                echo "command usage: php main.php [start|stop|restart|status]\n";
        }
    }

    /**
     * 启动
     * @return void
     */
    private function start(): void
    {
        if (!!$masterPid = $this->getMasterPid()) {
            echo "process already running\n";
            return;
        }

        $this->daemon();
        $masterPid = getmypid();
        $this->storeMasterPid($masterPid);
        $this->log('master started, pid=', $masterPid);

        foreach ($this->jobs as &$job) {
            $this->forkWorker($job, false, $masterPid);
            unset($job);
        }

        $sleep = 1000000;
        $lastCheckOverstockTime = 0;
        $continue = true;
        while (true) {
            if ($continue && $this->getMasterPid() !== $masterPid) {
                $continue = false;
                $sleep = 100000;
            }

            $status = 0;
            if ((0 < $workerPid = pcntl_waitpid(-1, $status, WNOHANG)) && (!!$job = $this->loadJob($workerPid))) {
                if (pcntl_wexitstatus($status) > 0) {
                    $this->log('worker exited with error, topic=', $job->topic, ', pid=', $workerPid, true);
                    $job->workerEnabledTime = time() + 60;
                } else {
                    $this->log('worker exited, topic=', $job->topic, ', pid=', $workerPid);
                }
                if ($continue && !$job->workers[$workerPid]['is_dynamic']) {
                    $this->forkWorker($job, false, $masterPid);
                }
                unset($job->workers[$workerPid]);
            }
            unset($job);

            if (-1 === $workerPid && !$continue) {
                break;
            }

            if (time() - $lastCheckOverstockTime > 60) {
                $this->dynamicFork($masterPid);
            }

            usleep($sleep);
        }
    }

    /**
     * 停止
     * @return boolean
     */
    private function stop(): bool
    {
        if (!$masterPid = $this->getMasterPid()) {
            echo 'process not running', PHP_EOL;
            return true;
        }
        $this->storeMasterPid(0);
        for ($i = 0; $i < 60; $i++) {
            if (!posix_kill($masterPid, 0)) {
                echo 'process stop success', PHP_EOL;
                $this->log('master stopped');
                return true;
            }
            usleep(500000);
        }

        echo 'process stop error', PHP_EOL;
        $this->log('master stop error', true);
        return false;
    }

    /**
     * 动态创建Worker子进程
     * @param integer $masterPid
     * @return void
     */
    public function dynamicFork(int $masterPid): void
    {
        foreach ($this->jobs as &$job) {
            if ($job->healthQueueLength > 0 && $job->dynamicWorkerCount > 0 && count($job->workers) <= $job->staticWorkerCount && $job->size() > $job->healthQueueLength) {
                for ($i = 0; $i < $job->dynamicWorkerCount; $i++) {
                    $this->forkWorker($job, true, $masterPid);
                }
            }
            unset($job);
        }
    }

    /**
     * 加载Worker
     * @param int $workerPid
     * @return Job|null
     */
    public function loadJob(int $workerPid): ?Job
    {
        foreach ($this->jobs as &$job) {
            if (isset($job->workers[$workerPid])) {
                return $job;
            }
        }
        return null;
    }

    /**
     * 创建Worker子进程
     * @param Job $job
     * @param boolean $isDynamic
     * @param int $masterPid
     * @return void
     */
    public function forkWorker(Job $job, bool $isDynamic, int $masterPid)
    {
        $pid = pcntl_fork();
        switch ($pid) {
            case -1:
                throw new Exception('fork worker error');
            case 0:
                try {
                    $startTime = time();
                    if ($job->workerEnabledTime > $startTime) {
                        $punishmentSleep = $job->workerEnabledTime - $startTime;
                        $this->log('worker started, but will punishment sleep ', $punishmentSleep, ' seconds, topic=', $job->topic, ', pid=', getmypid());
                        sleep($punishmentSleep);
                    } else {
                        $this->log('worker started, topic=', $job->topic, ', pid=', getmypid());
                    }
                    while (true) {
                        if (null !== $message = $job->brPop()) {
                            $job->doJob($message);
                        }
                        if ($this->getMasterPid() !== $masterPid || (time() - $startTime) > $job->maxExecuteTime) {
                            break;
                        }
                    }
                } catch (Throwable $e) {
                    throw $e;
                }
                exit(0);
            default:
                $job->workers[$pid] = [
                    'is_dynamic' => $isDynamic
                ];
        }
    }

    /**
     * 存储主进程ID
     * @param int $masterPid
     * @return void
     */
    public function storeMasterPid(int $masterPid): void
    {
        try {
            $dir = dirname($this->masterPidFile);
            clearstatcache(true, $dir);
            if (!is_dir($dir)) {
                mkdir($dir, 0777, true);
            }
            file_put_contents($this->masterPidFile, $masterPid);
        } catch (Throwable $e) {
            throw $e;
        }

    }

    /**
     * 加载文件主进程ID
     * @return int
     */
    public function getMasterPid(): int
    {
        try {
            clearstatcache(true, $this->masterPidFile);
            if (is_file($this->masterPidFile) && is_readable($this->masterPidFile) && (!!$masterPid = file_get_contents($this->masterPidFile)) && is_numeric($masterPid)) {
                $masterPid = intval($masterPid);
                return posix_kill($masterPid, 0) ? $masterPid : 0;
            }
            return 0;
        } catch (Throwable $e) {
            throw $e;
        }
    }

    /**
     * 写日志
     * @param string $content
     * @param boolean $isError
     * @return void
     */
    public function log(...$data)
    {
        try {
            clearstatcache();
            if (!is_file($this->logFile)) {
                $dirname = dirname($this->logFile);
                if (!is_dir($dirname)) {
                    mkdir($dirname, 0777, true);
                }
            }

            $isError = end($data);
            if (is_bool($isError)) {
                array_pop($data);
            } else {
                $isError = false;
            }

            $content = sprintf("[%s%s][%s]%s\n", date('Y-m-d H:i:s'), strstr(strval(microtime(true)), '.'), $isError ? 'ERROR' : 'INFO', implode('', $data));

            if (is_file($this->logFile) && filesize($this->logFile) > 10485760) {
                $basename = basename($this->logFile);
                $suffix = '';
                $logFilePart = dirname($this->logFile) . '/';
                if (false === $index = strrpos($basename, '.')) {
                    $logFilePart .= $basename;
                } else {
                    $logFilePart .= substr($basename, 0, $index);
                    $suffix = substr($basename, $index);
                }
                for ($i = 5; $i >= 2; $i--) {
                    $logFileLeft .= ($i - 1) . $suffix;
                    $logFileRight .= $i . $suffix;
                    if (is_file($logFileLeft)) {
                        rename($logFileLeft, $logFileRight);
                    }
                }
                rename($this->logFile, $logFileLeft);
            }

            if (false !== ($fp = fopen($this->logFile, 'a'))) {
                flock($fp, LOCK_EX);
                fwrite($fp, $content);
                flock($fp, LOCK_UN);
                fclose($fp);
            }

        } catch (Throwable $e) {
            // no code
        }
    }

    /**
     * 守护
     * @return void
     */
    public function daemon()
    {
        $pid = pcntl_fork();
        switch ($pid) {
            case -1:
                $this->log('fork failed');
                exit(1);
            case 0:
                if (posix_setsid() <= 0) {
                    $this->log('set sid failed');
                    exit(1);
                }
                if (chdir('/') === false) {
                    $this->log('change directory failed');
                    exit(1);
                }
                umask(0);
                fclose(STDIN);
                // fclose(STDOUT);
                // fclose(STDERR);
                break;
            default:
                exit(0);
        }
    }
}