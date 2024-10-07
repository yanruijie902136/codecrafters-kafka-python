from __future__ import annotations

import io
import uuid
from typing import Generator, Optional

from .partition_record import PartitionRecord
from .record_batch import RecordBatch
from .topic_record import TopicRecord


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class RecordManager(metaclass=SingletonMeta):
    def __init__(self) -> None:
        self._topic_records: list[TopicRecord] = []
        self._partition_records: list[PartitionRecord] = []

        with io.open(
            "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log", mode="rb"
        ) as byte_stream:
            while byte_stream.peek():
                record_batch = RecordBatch.decode(byte_stream)
                for record in record_batch.records:
                    if type(record) is TopicRecord:
                        self._topic_records.append(record)
                    elif type(record) is PartitionRecord:
                        self._partition_records.append(record)

    def get_topic(self, topic_name: str) -> Optional[TopicRecord]:
        for topic_record in self._topic_records:
            if topic_record.name == topic_name:
                return topic_record

    def get_partitions(self, topic_id: uuid.UUID) -> Generator[PartitionRecord, None, None]:
        for partition_record in self._partition_records:
            if partition_record.topic_id == topic_id:
                yield partition_record
