<?php

namespace ProophTest\EventStore\Snapshot\Adpater\Redis;

use Predis\Client;
use Prooph\EventStore\Aggregate\AggregateType;
use Prooph\EventStore\Snapshot\Adapter\Redis\RedisSnapshotAdapter;
use Prooph\EventStore\Snapshot\Snapshot;

/**
 * @author Eric Braun <eb@oqq.be>
 */
final class RedisSnapshotAdapterTest extends \PHPUnit_Framework_TestCase
{
    /** @var Client */
    private $client;

    /** @var RedisSnapshotAdapter */
    private $adapter;

    /**
     * @inheritdoc
     */
    public function setUp()
    {
        $this->client = new Client(null, ['prefix' => str_replace('\\', '', __CLASS__)]);

        $this->adapter = new RedisSnapshotAdapter($this->client);
    }

    /**
     * @inheritdoc
     */
    protected function tearDown()
    {
        if (null === $this->client) {
            return;
        }

        $prefix = $this->client->getOptions()->__get('prefix')->getPrefix();
        $prefixLen = strlen($prefix);

        // we have to use array_filter, since keys($prefix . '*') does not work how expected :(
        $keys = array_filter($this->client->keys('*'), function ($value) use ($prefix) {
            return 0 === strpos($value, $prefix);
        });

        if ($keys) {
            $this->client->del(array_map(function ($value) use ($prefixLen) {
                return substr($value, $prefixLen);
            }, $keys));
        }
    }

    /**
     * @test
     */
    public function it_saves_and_reads()
    {
        $aggregateType = AggregateType::fromString('foo');

        $aggregateRoot = new \stdClass();
        $aggregateRoot->foo = 'bar';

        $time = microtime(true);

        if (false === strpos($time, '.')) {
            $time .= '.0000';
        }

        $now = \DateTimeImmutable::createFromFormat('U.u', $time, new \DateTimeZone('UTC'));

        $snapshot = new Snapshot($aggregateType, 'id', $aggregateRoot, 1, $now);
        $this->adapter->save($snapshot);

        $this->assertNull($this->adapter->get($aggregateType, 'invalid'));

        $readSnapshot = $this->adapter->get($aggregateType, 'id');
        $this->assertEquals($snapshot, $readSnapshot);
    }
}
