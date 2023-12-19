<?php

declare(strict_types=1);

namespace Limphp\Example;

use Limphp\Jobs\Job;

class TestJob2 extends Job
{

    public $topic = 'test2';
    public $isDelay = false;
    public $staticWorkerCount = 1;
    public $dynamicWorkerCount = 4;
    public $healthQueueLength = 100;
    public $maxExecuteTime = 100;

    public function getQueueConfig(): array
    {
        return [
            'host' => '127.0.0.1',
            'password' => '',
            'port' => 6379,
            'database' => 0,
        ];
    }

    public function doJob(string $message): void
    {
        sleep(1);
    }

}