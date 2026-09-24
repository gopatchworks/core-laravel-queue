<?php

namespace Enqueue\LaravelQueue;

use Interop\Amqp\AmqpContext;

/**
 * @method AmqpContext getQueueInteropContext()
 */
class AmqpQueue extends Queue
{
    /**
     * @var int
     */
    protected $size = 0;

    /**
     * @var array<string, true>
     */
    protected $declaredQueues = [];

    /**
     * {@inheritdoc}
     *
     * @param AmqpContext $amqpContext
     */
    public function __construct(AmqpContext $amqpContext, $queueName, $timeToRun)
    {
        parent::__construct($amqpContext, $queueName, $timeToRun);
    }

    /**
     * {@inheritdoc}
     */
    public function size($queue = null)
    {
        $this->declareQueue($queue);

        return $this->size;
    }

    /**
     * {@inheritdoc}
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->declareQueueOnce($queue);

        parent::pushRaw($payload, $queue, $options);
    }

    /**
     * {@inheritdoc}
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        $this->declareQueueOnce($queue);

        return parent::later($delay, $job, $data, $queue);
    }

    /**
     * {@inheritdoc}
     */
    public function pop($queue = null)
    {
        $this->declareQueue($queue);

        return parent::pop($queue);
    }

    /**
     * A declare blocks on a broker round trip, and queues are durable, so once per process is enough.
     * Gotcha: a queue deleted while this process runs is not re-created, and publishes to it are dropped.
     *
     * @param string|null $queue
     */
    protected function declareQueueOnce($queue = null)
    {
        $name = $this->getQueue($queue)->getQueueName();

        if (isset($this->declaredQueues[$name])) {
            return;
        }

        $this->declareQueue($queue);
        $this->declaredQueues[$name] = true;
    }

    /**
     * @param string|null $queue
     */
    protected function declareQueue($queue = null)
    {
        $interopQueue = $this->getQueue($queue);
        $interopQueue->addFlag(\Interop\Amqp\AmqpQueue::FLAG_DURABLE);
		$interopQueue->setArgument('x-queue-type', 'quorum');

        $this->size = $this->getQueueInteropContext()->declareQueue($interopQueue);
    }
}
