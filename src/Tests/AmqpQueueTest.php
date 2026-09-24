<?php

declare(strict_types=1);

namespace Enqueue\LaravelQueue\Tests;

use Enqueue\LaravelQueue\AmqpQueue;
use Illuminate\Container\Container;
use Illuminate\Support\Facades\Facade;
use Interop\Amqp\AmqpContext;
use Interop\Amqp\Impl\AmqpMessage;
use Interop\Amqp\Impl\AmqpQueue as InteropAmqpQueue;
use Interop\Queue\Producer as InteropProducer;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

class AmqpQueueTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        $app = new Container();
        $app->instance('log', new class {
            public function __call($name, $args) { return $this; }
        });
        Facade::setFacadeApplication($app);
    }

    protected function tearDown(): void
    {
        Facade::clearResolvedInstances();
        Facade::setFacadeApplication(null);
        parent::tearDown();
    }

    public function testShouldDeclareQueueOnlyOnceForRepeatedPushes(): void
    {
        $context = $this->createAmqpContextMock();
        $context
            ->expects($this->once())
            ->method('declareQueue')
            ->willReturn(0)
        ;

        $queue = new AmqpQueue($context, 'default', 0);

        $queue->pushRaw('first', 'gateway');
        $queue->pushRaw('second', 'gateway');
        $queue->pushRaw('third', 'gateway');
    }

    public function testShouldDeclareEachQueueOnce(): void
    {
        $declared = [];

        $context = $this->createAmqpContextMock();
        $context
            ->expects($this->exactly(2))
            ->method('declareQueue')
            ->willReturnCallback(function (InteropAmqpQueue $queue) use (&$declared) {
                $declared[] = $queue->getQueueName();

                return 0;
            })
        ;

        $queue = new AmqpQueue($context, 'default', 0);

        $queue->pushRaw('first', 'gateway');
        $queue->pushRaw('second', 'start');
        $queue->pushRaw('third', 'gateway');
        $queue->pushRaw('fourth', 'start');

        $this->assertSame(['gateway', 'start'], $declared);
    }

    public function testShouldDeclareDefaultQueueOnceWhenNoQueueGiven(): void
    {
        $context = $this->createAmqpContextMock();
        $context
            ->expects($this->once())
            ->method('declareQueue')
            ->with($this->callback(fn (InteropAmqpQueue $queue) => 'default' === $queue->getQueueName()))
            ->willReturn(0)
        ;

        $queue = new AmqpQueue($context, 'default', 0);

        $queue->pushRaw('first');
        $queue->pushRaw('second', 'default');
    }

    public function testShouldDeclareQueueOnlyOnceForRepeatedDelayedPushes(): void
    {
        $context = $this->createAmqpContextMock();
        $context
            ->expects($this->once())
            ->method('declareQueue')
            ->willReturn(0)
        ;

        $queue = new AmqpQueue($context, 'default', 0);

        $queue->later(10, 'SomeJob', '', 'gateway');
        $queue->later(10, 'SomeJob', '', 'gateway');
        $queue->pushRaw('immediate', 'gateway');
    }

    public function testShouldDeclareQueueOnEverySizeCall(): void
    {
        $context = $this->createAmqpContextMock();
        $context
            ->expects($this->exactly(3))
            ->method('declareQueue')
            ->willReturnOnConsecutiveCalls(0, 5, 7)
        ;

        $queue = new AmqpQueue($context, 'default', 0);

        $queue->pushRaw('first', 'gateway');

        $this->assertSame(5, $queue->size('gateway'));
        $this->assertSame(7, $queue->size('gateway'));
    }

    public function testShouldRetryDeclareAfterItFails(): void
    {
        $context = $this->createAmqpContextMock();
        $context
            ->expects($this->exactly(2))
            ->method('declareQueue')
            ->willReturnCallback(function () {
                static $calls = 0;

                if (1 === ++$calls) {
                    throw new \RuntimeException('Broker unavailable');
                }

                return 0;
            })
        ;

        $queue = new AmqpQueue($context, 'default', 0);

        try {
            $queue->pushRaw('first', 'gateway');
            $this->fail('Expected the first declare to throw.');
        } catch (\RuntimeException) {
        }

        $queue->pushRaw('second', 'gateway');
        $queue->pushRaw('third', 'gateway');
    }

    private function createAmqpContextMock(): AmqpContext&MockObject
    {
        $context = $this->createMock(AmqpContext::class);
        $context
            ->method('createQueue')
            ->willReturnCallback(fn (string $name) => new InteropAmqpQueue($name))
        ;
        $context
            ->method('createMessage')
            ->willReturnCallback(fn (string $body) => new AmqpMessage($body))
        ;
        $context
            ->method('createProducer')
            ->willReturn($this->createMock(InteropProducer::class))
        ;

        return $context;
    }
}
