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
     * 进程名前缀
     * @var string
     */
    private $processTitle = '';

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
     * @param string $processTitle
     */
    public function __construct(string $processTitle, string $runtimeDir)
    {
        $this->processTitle = $processTitle;
        $this->masterPidFile = rtrim($runtimeDir, '/') . '/master.pid';
        $this->logFile = rtrim($runtimeDir, '/') . '/logs/process.log';

        set_exception_handler(function ($e) {
            $this->log($e->getMessage(), $e->getTraceAsString(), true);
            exit(1);
        });

        set_error_handler(function (int $errorNo, string $errorMsg) {
            $this->log($errorMsg, true);
            exit(1);
        });
    }

    /**
     * 初始化
     * @param string $processTitle
     * @param string $runtimeDir
     * @return self
     */
    public static function init(string $processTitle, string $runtimeDir): self
    {
        return new self($processTitle, $runtimeDir);
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
            $job->maxExecuteTime = intval($job->maxExecuteTime);
            $job->maxConsumeCount = intval($job->maxConsumeCount);
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
        $masterPid = $this->getRunningMasterPid();
        $this->log('cmd ', $cmd);
        switch ($cmd) {
            case 'start':
                if (!!$masterPid) {
                    echo "process already running\n";
                    return;
                }
                $this->start();
                break;
            case 'stop':
                if (!$masterPid) {
                    echo 'process not running', PHP_EOL;
                    return;
                }
                $this->stop($masterPid);
                break;
            case 'restart':
                if (!!$masterPid) {
                    if ($this->stop($masterPid)) {
                        $this->start();
                    }
                } else {
                    $this->start();
                }
                break;
            case 'status':
                if (!!$masterPid) {
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
        cli_set_process_title($this->processTitle . ':master');
        $this->daemon();
        $masterPid = getmypid();
        $this->storeMasterPid($masterPid);
        echo "process start success\n";
        $this->log('master start');

        foreach ($this->jobs as &$job) {
            $this->forkWorker($job, false, $masterPid);
            unset($job);
        }

        $lastTime = 0;
        $sleep = 1000000;
        $exitSignal = false;
        pcntl_signal(SIGUSR1, function ($signo) use (&$exitSignal, &$sleep) {
            $exitSignal = true;
            $sleep = 100000;
            foreach ($this->jobs as $job) {
                foreach ($job->workers as $workerPid => $nouse) {
                    posix_kill($workerPid, SIGUSR1);
                }
            }
        });
        while (true) {
            pcntl_signal_dispatch();

            $time = time();
            $status = 0;
            if ((0 < $workerPid = pcntl_waitpid(-1, $status, WNOHANG)) && (!!$job = $this->loadJob($workerPid))) {
                if (pcntl_wexitstatus($status) > 0) {
                    $this->log('worker exit with error, pid=', $workerPid, true);
                    $job->workerEnabledTime = $time + 60;
                } else {
                    $this->log('worker exit, pid=', $workerPid);
                }
                if (!$exitSignal && !$job->workers[$workerPid]['is_dynamic']) {
                    $this->forkWorker($job, false, $masterPid);
                }
                unset($job->workers[$workerPid]);
            }
            unset($job);

            if ($exitSignal && -1 === $workerPid) {
                break;
            }
            if (!$exitSignal && $time - $lastTime > 60) {
                $lastTime = $time;
                if ($this->getRunningMasterPid() !== $masterPid) {
                    $this->log('process file pid not match, master will exit', true);
                    posix_kill($masterPid, SIGUSR1);
                } else {
                    $this->dynamicFork($masterPid);
                }
            }

            usleep($sleep);
        }
        $this->log('master exit');
    }

    /**
     * 停止
     * @param integer $masterPid
     * @return boolean
     */
    private function stop($masterPid): bool
    {
        $this->storeMasterPid(0);
        posix_kill($masterPid, SIGUSR1);
        for ($i = 0; $i < 60; $i++) {
            if (!posix_kill($masterPid, 0)) {
                echo 'process stop success', PHP_EOL;
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
                    cli_set_process_title($this->processTitle . ':worker');
                    $startTime = time();
                    $consumeCount = 0;
                    if ($job->workerEnabledTime > $startTime) {
                        $this->log('worker start, but will punishment sleep ', ($job->workerEnabledTime - $startTime), ' seconds');
                    } else {
                        $this->log('worker start');
                    }
                    $exitSignal = false;
                    pcntl_signal(SIGUSR1, function ($signo) use (&$exitSignal) {
                        $exitSignal = true;
                    });
                    while (true) {
                        pcntl_signal_dispatch();
                        if (posix_getppid() !== $masterPid) {
                            $this->log('worker found master died, worker will exit', true);
                            break;
                        }
                        if ($exitSignal || ($job->maxExecuteTime > 0 && (time() - $startTime) > $job->maxExecuteTime) || ($job->maxConsumeCount > 0 && $consumeCount > $job->maxConsumeCount)) {
                            break;
                        }
                        if ($job->workerEnabledTime > time()) {
                            usleep(1000000);
                        } else {
                            if (null !== $message = $job->brPop()) {
                                $job->doJob($message);
                                $consumeCount++;
                            }
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
     * 刷新存储主进程ID到文件
     * @param int $masterPid
     * @return void
     */
    public function storeMasterPid(int $masterPid): void
    {
        $dir = dirname($this->masterPidFile);
        clearstatcache(true, $dir);
        if (!is_dir($dir)) {
            mkdir($dir, 0777, true);
        }
        file_put_contents($this->masterPidFile, $masterPid);
    }

    /**
     * 获取正在运行的主进程ID
     * @return integer
     */
    public function getRunningMasterPid(): int
    {
        clearstatcache(true, $this->masterPidFile);
        if (is_file($this->masterPidFile) && is_readable($this->masterPidFile) && (!!$masterPid = file_get_contents($this->masterPidFile)) && is_numeric($masterPid) && (1 <= $masterPid = intval($masterPid))) {
            return posix_kill($masterPid, 0) ? $masterPid : 0;
        }
        return 0;
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

            $content = sprintf("[%s%s][%s][pid=%d]%s\n", date('Y-m-d H:i:s'), substr(sprintf('%01.4f', explode(' ', microtime())[0]), 1), $isError ? 'ERROR' : 'INFO', getmypid(), str_replace("\n", ' ', implode('', $data)));

            if (is_file($this->logFile) && filesize($this->logFile) > 10485760) {
                if (false !== ($fp = fopen($this->logFile, 'a'))) {
                    if (flock($fp, LOCK_EX | LOCK_NB)) {
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
                            $logFileLeft = $logFilePart . ($i - 1) . $suffix;
                            $logFileRight = $logFilePart . $i . $suffix;
                            if (is_file($logFileLeft)) {
                                rename($logFileLeft, $logFileRight);
                            }
                        }
                        rename($this->logFile, $logFileLeft);
                        flock($fp, LOCK_UN);
                    }
                    fclose($fp);
                }
            }

            file_put_contents($this->logFile, $content, FILE_APPEND | LOCK_EX);
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
                $this->log('fork failed', true);
                exit(1);
            case 0:
                if (posix_setsid() <= 0) {
                    $this->log('set sid failed', true);
                    exit(1);
                }
                if (chdir('/') === false) {
                    $this->log('change directory failed', true);
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