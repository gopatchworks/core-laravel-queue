<?php

namespace Enqueue\LaravelQueue;

use Enqueue\Consumption\ChainExtension;
use Enqueue\Consumption\Context\MessageReceived;
use Enqueue\Consumption\Context\MessageResult;
use Enqueue\Consumption\Context\PostMessageReceived;
use Enqueue\Consumption\Context\PreConsume;
use Enqueue\Consumption\Context\Start;
use Enqueue\Consumption\Extension\LimitConsumedMessagesExtension;
use Enqueue\Consumption\MessageReceivedExtensionInterface;
use Enqueue\Consumption\MessageResultExtensionInterface;
use Enqueue\Consumption\PostMessageReceivedExtensionInterface;
use Enqueue\Consumption\PreConsumeExtensionInterface;
use Enqueue\Consumption\QueueConsumer;
use Enqueue\Consumption\Result;
use Enqueue\Consumption\StartExtensionInterface;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Contracts\Queue\Factory as QueueManager;
use Illuminate\Queue\WorkerOptions;
use Illuminate\Support\Facades\Log;

class Worker extends \Illuminate\Queue\Worker implements
    StartExtensionInterface,
    PreConsumeExtensionInterface,
    MessageReceivedExtensionInterface,
    PostMessageReceivedExtensionInterface,
    MessageResultExtensionInterface
{
    protected $connectionName;

    protected $queueNames;

    protected $queue;

    protected $options;

    protected $lastRestart;

    protected $interop = false;

    protected $stopped = false;

    protected $job;

    protected $extensions = [];

    public function __construct(
        QueueManager $manager,
        Dispatcher $events,
        ExceptionHandler $exceptions,
        callable $isDownForMaintenance,
        ?callable $resetScope = null,
    ) {
        parent::__construct($manager, $events, $exceptions, $isDownForMaintenance, $resetScope);

        Log::withContext(['unique_worker_key' => mt_rand(1_000_000, 99_999_999)]);
    }

    public function daemon($connectionName, $queueNames, WorkerOptions $options)
    {
        Log::info('[Worker] Daemon started.', [
            'options' => [
                'name' => $options->name,
                'backoff' => $options->backoff,
                'sleep' => $options->sleep,
                'rest' => $options->rest,
                'force' => $options->force,
                'memory' => $options->memory,
                'timeout' => $options->timeout,
                'maxTries' => $options->maxTries,
                'stopWhenEmpty' => $options->stopWhenEmpty,
                'maxJobs' => $options->maxJobs,
                'maxTime' => $options->maxTime,
            ]
        ]);

        $this->connectionName = $connectionName;
        $this->queueNames = $queueNames;
        $this->options = $options;

        /** @var Queue $queue */
        $this->queue = $this->getManager()->connection($connectionName);
        $this->interop = $this->queue instanceof Queue;

        if (false == $this->interop) {
            Log::info('[Worker] Parent Daemon started.');
            parent::daemon($connectionName, $this->queueNames, $options);
            return;
        }

        $context = $this->queue->getQueueInteropContext();
        $queueConsumer = new QueueConsumer($context, new ChainExtension(
            $this->getAllExtensions([$this])
        ));
        foreach (explode(',', $queueNames) as $queueName) {
            $queueConsumer->bindCallback($queueName, function () {
                try {
                    $this->runJob($this->job, $this->connectionName, $this->options);
                } catch (\Throwable $e) {
                    Log::critical('[Worker] Unhandled exception during job execution.', [
                        'job_id' => optional($this->job)->getJobId(),
                        'job_class' => optional($this->job)->resolveName(),
                        'exception' => $e->getMessage(),
                        'trace' => $e->getTraceAsString(),
                    ]);

                    throw $e;
                }

                Log::debug('[Worker] ran callback.');
                return Result::ALREADY_ACKNOWLEDGED;
            });
        }

        $queueConsumer->consume();
    }

    public function runNextJob($connectionName, $queueNames, WorkerOptions $options)
    {
        Log::debug('[Worker] Run next job.');

        $this->connectionName = $connectionName;
        $this->queueNames = $queueNames;
        $this->options = $options;

        /** @var Queue $queue */
        $this->queue = $this->getManager()->connection($connectionName);
        $this->interop = $this->queue instanceof Queue;

        if (false == $this->interop) {
            Log::info('[Worker] Parent Run next job.');
            parent::runNextJob($connectionName, $this->queueNames, $options);
            return;
        }

        $context = $this->queue->getQueueInteropContext();

        $queueConsumer = new QueueConsumer($context, new ChainExtension($this->getAllExtensions([
            $this,
            new LimitConsumedMessagesExtension(1),
        ])));

        Log::debug('[Worker] create new queue consumer with 1 message limit');
        foreach (explode(',', $queueNames) as $queueName) {
            $queueConsumer->bindCallback($queueName, function () {
                $this->runJob($this->job, $this->connectionName, $this->options);

                Log::debug('[Worker] ran next callback.');
                return Result::ALREADY_ACKNOWLEDGED;
            });
        }

        $queueConsumer->consume();
    }

    public function onStart(Start $context): void
    {
        Log::info('[Worker] Starting daemon.', [
            'connection' => $this->connectionName,
            'queues' => $this->queueNames,
        ]);

        if ($this->supportsAsyncSignals()) {
            $this->listenForSignals();
        }

        $this->lastRestart = $this->getTimestampOfLastQueueRestart();

        if ($this->stopped) {
            $context->interruptExecution();
        }
    }

    public function onPreConsume(PreConsume $context): void
    {
        if (!$this->daemonShouldRun($this->options, $this->connectionName, $this->queueNames)) {
            Log::info('[Worker] Pausing worker.', ['options' => $this->options]);
            $this->pauseWorker($this->options, $this->lastRestart);
        }

        if ($this->stopped) {
            Log::info('[Worker] Execution interrupted due to stop signal 185.');
            $context->interruptExecution();
        }
    }

    public function onMessageReceived(MessageReceived $context): void
    {
        $this->job = $this->queue->convertMessageToJob(
            $context->getMessage(),
            $context->getConsumer()
        );

        Log::info('[Worker] Received new job.', [
            'job_id' => $this->job->getJobId(),
            'job_class' => $this->job->resolveName(),
        ]);

        if ($this->supportsAsyncSignals()) {
            $this->registerTimeoutHandler($this->job, $this->options);
        }
    }

    public function onPostMessageReceived(PostMessageReceived $context): void
    {
        Log::info('[Worker] Finished processing job, checking for stop.', [
            'job_id' => $this->job->getJobId(),
            'job_class' => $this->job->resolveName(),
        ]);

        $this->stopIfNecessary($this->options, $this->lastRestart, $this->job);

        Log::info('[Worker] Finished processing job, not stopping.');

        if ($this->stopped) {
            Log::info('[Worker] Execution interrupted due to stop signal on 217.');
            $context->interruptExecution();
        }

        Log::info('[Worker] Finished processing job, not stopped.');
    }

    public function onResult(MessageResult $context): void
    {
        Log::info('[Worker] Job result processed.', [
            'job_id' => $context->getMessage()->getMessageId(),
            'result' => $context->getResult(),
        ]);

        if ($this->supportsAsyncSignals()) {
            $this->resetTimeoutHandler();
        }
    }

    public function stop($status = 0, $options = null)
    {
        if ($this->interop) {
            Log::info('[Worker] Received stop signal. Worker will exit after the current job.');
            $this->stopped = true;

            return;
        }

        parent::stop($status, $options);
    }

    public function setExtensions(array $extensions): self
    {
        $this->extensions = $extensions;

        return $this;
    }

    protected function getAllExtensions(array $array): array
    {
        foreach ($this->extensions as $extension) {
            $array[] = $extension;
        }

        return $array;
    }
}
