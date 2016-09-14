<?php

namespace Prooph\EventStore\Snapshot\Adapter\Redis;

use DateTimeImmutable;
use DateTimeZone;
use Predis\ClientInterface;
use Prooph\EventStore\Aggregate\AggregateType;
use Prooph\EventStore\Snapshot\Adapter\Adapter;
use Prooph\EventStore\Snapshot\Snapshot;

/**
 * @author Eric Braun <eb@oqq.be>
 */
final class RedisSnapshotAdapter implements Adapter
{
    /** @var ClientInterface */
    private $redis;

    /**
     * @param ClientInterface $redis
     */
    public function __construct(ClientInterface $redis)
    {
        $this->redis = $redis;
    }

    /**
     * @inheritdoc
     */
    public function get(AggregateType $aggregateType, $aggregateId)
    {
        $key = $this->createRedisKeyFromAggregate($aggregateType, $aggregateId);
        $data = $this->redis->hgetall($key);

        if (!$data) {
            return;
        }

        return new Snapshot(
            $aggregateType,
            $aggregateId,
            unserialize($data['aggregate_root']),
            $data['last_version'],
            DateTimeImmutable::createFromFormat('U.u', $data['created_at'], new DateTimeZone('UTC'))
        );
    }

    /**
     * @inheritdoc
     */
    public function save(Snapshot $snapshot)
    {
        $key = $this->createRedisKeyFromAggregate($snapshot->aggregateType(), $snapshot->aggregateId());

        $data = [
            'aggregate_root' => serialize($snapshot->aggregateRoot()),
            'aggregate_type' => $snapshot->aggregateType()->toString(),
            'aggregate_id' => $snapshot->aggregateId(),
            'last_version' => $snapshot->lastVersion(),
            'created_at' => $snapshot->createdAt()->format('U.u'),
        ];

        $this->redis->hmset($key, $data);
    }

    /**
     * @param AggregateType $aggregateType
     * @param string        $aggregateId
     *
     * @return string
     */
    private function createRedisKeyFromAggregate(AggregateType $aggregateType, string $aggregateId): string
    {
        return sprintf(
            '{%s}:%s',
            implode('', array_slice(explode('\\', $aggregateType->toString()), -1)),
            $aggregateId
        );
    }
}
