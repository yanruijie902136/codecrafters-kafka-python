__all__ = "ClusterMetadata",

import io
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional, Type
from uuid import UUID

from .protocol.metadata import PartitionRecord, TopicRecord, read_metadata_record
from .protocol.record import RecordBatch


@dataclass
class _TopicPartition:
    partition_index: int
    leader_id: int
    leader_epoch: int
    replicas: List[int]
    isr: List[int]


@dataclass
class _Topic:
    name: str
    topic_id: UUID
    partitions: Dict[int, _TopicPartition] = field(default_factory=dict)


class _SingletonMeta(type):
    _instances: ClassVar[Dict[Type, Any]] = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class ClusterMetadata(metaclass=_SingletonMeta):
    def __init__(self) -> None:
        self._topics_by_name: Dict[str, _Topic] = {}
        self._topics_by_id: Dict[UUID, _Topic] = {}

        with open("/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", mode="rb") as stream:
            while stream.peek(1):
                record_batch = RecordBatch.read(stream)
                for record in record_batch.records:
                    if record.value is None:
                        continue
                    substream = io.BytesIO(record.value)
                    metadata_record = read_metadata_record(substream)
                    if isinstance(metadata_record, PartitionRecord):
                        topic = self._topics_by_id[metadata_record.topic_id]
                        partition = _TopicPartition(
                            partition_index=metadata_record.partition_id,
                            leader_id=metadata_record.leader,
                            leader_epoch=metadata_record.leader_epoch,
                            replicas=metadata_record.replicas,
                            isr=metadata_record.isr,
                        )
                        topic.partitions[metadata_record.partition_id] = partition
                    elif isinstance(metadata_record, TopicRecord):
                        topic = _Topic(
                            name=metadata_record.name,
                            topic_id=metadata_record.topic_id,
                        )
                        self._topics_by_name[metadata_record.name] = topic
                        self._topics_by_id[metadata_record.topic_id] = topic

    def get_topic(self, name: Optional[str] = None, topic_id: Optional[UUID] = None) -> Optional[_Topic]:
        if name is not None:
            return self._topics_by_name.get(name)
        if topic_id is not None:
            return self._topics_by_id.get(topic_id)
        return None
