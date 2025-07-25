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
            Log::debug('[Job] Acknowledging message.', ['job_id' => $this->getJobId()]);
            $this->consumer->acknowledge($this->message);
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
        parent::release($delay);

        try {
            $requeueMessage = clone $this->message;
            $requeueMessage->setProperty('x-attempts', $this->attempts() + 1);

            $producer = $this->context->createProducer();

            try {
                $producer->setDeliveryDelay($this->secondsUntil($delay) * 1000);
            } catch (DeliveryDelayNotSupportedException $e) {
            }

            Log::debug('[Job] Releasing job by acknowledging old message and sending new one.', [
                'job_id' => $this->getJobId()
            ]);

            $this->consumer->acknowledge($this->message);
            $producer->send($this->consumer->getQueue(), $requeueMessage);

        } catch (\Throwable $e) {
            Log::critical('[Job] Failed to release job. Potential for lost job.', [
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
