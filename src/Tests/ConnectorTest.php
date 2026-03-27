<?php

namespace Enqueue\LaravelQueue\Tests;

use Enqueue\LaravelQueue\Connector;
use Enqueue\LaravelQueue\Queue;
use Enqueue\Null\NullContext;
use Enqueue\Test\ClassExtensionTrait;
use Illuminate\Container\Container;
use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Support\Facades\Facade;
use Interop\Queue\Queue as InteropQueue;
use PHPUnit\Framework\TestCase;

class ConnectorTest extends TestCase
{
    use ClassExtensionTrait;

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

    public function testShouldImplementConnectorInterface()
    {
        $this->assertClassImplements(ConnectorInterface::class, Connector::class);
    }

    public function testCouldBeConstructedWithoutAnyArguments()
    {
        new Connector();
    }

    public function testShouldReturnQueueOnConnectMethodCall()
    {
        $connector = new Connector();

        $this->assertInstanceOf(Queue::class, $connector->connect([
            'dsn' => 'null://',
        ]));
    }

    public function testShouldSetExpectedOptionsIfNotProvidedOnConnectMethodCall()
    {
        $connector = new Connector();

        $queue = $connector->connect(['dsn' => 'null://']);

        $this->assertInstanceOf(NullContext::class, $queue->getQueueInteropContext());

        $this->assertInstanceOf(InteropQueue::class, $queue->getQueue());
        $this->assertSame('default', $queue->getQueue()->getQueueName());

        $this->assertSame(0, $queue->getTimeToRun());
    }

    public function testShouldSetExpectedCustomOptionsIfProvidedOnConnectMethodCall()
    {
        $connector = new Connector();

        $queue = $connector->connect([
            'dsn' => 'null://',
            'queue' => 'theCustomQueue',
            'time_to_run' => 123,
        ]);

        $this->assertInstanceOf(NullContext::class, $queue->getQueueInteropContext());

        $this->assertInstanceOf(InteropQueue::class, $queue->getQueue());
        $this->assertSame('theCustomQueue', $queue->getQueue()->getQueueName());

        $this->assertSame(123, $queue->getTimeToRun());
    }
}
