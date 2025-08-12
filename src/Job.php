<?php

namespace Enqueue\LaravelQueue;

use Illuminate\Container\Container;
use Illuminate\Contracts\Queue\Job as JobContract;
use Illuminate\Queue\Jobs\Job as BaseJob;
use Interop\Queue\Consumer;
use Interop\Queue\Context;
use Interop\Queue\Exception\DeliveryDelayNotSupportedException;
use Interop\Queue\Message;
use Illuminate\Support\Facades\Log;

class Job extends BaseJob implements JobContract
{
    /**
     * @var Context
     */
    private $context;

    /**
     * @var Consumer
     */
    private $consumer;

    /**
     * @var Message
     */
    private $message;

    public function __construct(Container $container, Context $context, Consumer $consumer, Message $message, $connectionName)
    {
        $this->container = $container;
        $this->context = $context;
        $this->consumer = $consumer;
        $this->message = $message;
        $this->connectionName = $connectionName;

        Log::withContext(['unique_job_key' => mt_rand(1_000_000,99_999_999)]);
    }

    public function getJobId()
    {
        return $this->message->getMessageId();
    }

    /**
     * {@inheritdoc}
     */
    public function delete()
    {
        parent::delete();

        try {
            Log::debug('[Job] Acknowledging message.');
            $this->consumer->acknowledge($this->message);
            Log::debug('[Job] Message acknowledged.');
        } catch (\Throwable $e) {
            Log::critical('[Job] Failed to acknowledge message.', [
                'job_id' => $this->getJobId(),
                'exception' => $e->getMessage(),
            ]);

            throw $e;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function release($delay = 0)
    {
        Log::debug('[Job] Releasing job.');
        parent::release($delay);
        Log::debug('[Job] Job released.');

        try {
            $requeueMessage = clone $this->message;
            $requeueMessage->setProperty('x-attempts', $this->attempts() + 1);

            $producer = $this->context->createProducer();

            try {
                $producer->setDeliveryDelay($this->secondsUntil($delay) * 1000);
            } catch (DeliveryDelayNotSupportedException $e) {
            }

            Log::debug('[Job] Releasing job by acknowledging old message and sending new one.');

            $this->consumer->acknowledge($this->message);
            $producer->send($this->consumer->getQueue(), $requeueMessage);

        } catch (\Throwable $e) {
            Log::debug('[Job] Failed to release job. Potential for lost job.', [
                'job_id' => $this->getJobId(),
                'exception' => $e->getMessage(),
            ]);

            throw $e;
        }
    }

    public function getQueue()
    {
        return $this->consumer->getQueue()->getQueueName();
    }

    public function attempts()
    {
        return $this->message->getProperty('x-attempts', 1);
    }

    public function getRawBody()
    {
        return $this->message->getBody();
    }
}
