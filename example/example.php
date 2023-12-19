<?php
use Limphp\Example\TestJob1;
use Limphp\Example\TestJob2;
use Limphp\Jobs\Process;

// 限制CLI模式下使用
if ('cli' !== php_sapi_name()) {
    echo 'service can only run in cli mode', PHP_EOL;
    exit;
}

// 自动加载
if (is_file(__DIR__ . '/../vendor/autoload.php')) {
    require_once __DIR__ . '/../vendor/autoload.php';
} else {
    spl_autoload_register(function ($class) {
        $file = __DIR__ . '/../' . str_replace('\\', '/', $class) . '.php';
        $file = str_replace('Limphp/Jobs', 'src', $file);
        $file = str_replace('Limphp/Example', 'example', $file);
        require_once $file;
    });
}

Process::init(__DIR__)
    ->registerJob(new TestJob1())
    ->registerJob(new TestJob2())
    ->execute($argv[1] ?? '');

