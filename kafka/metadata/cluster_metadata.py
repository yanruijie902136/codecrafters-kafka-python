import io
import uuid
from collections import defaultdict

from ..primitive_types import (
    decode_compact_string,
    decode_int8,
    decode_int32,
    decode_uuid
)

from .record import Record
from .record_batch import RecordBatch
from .record_type import RecordType


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class ClusterMetadata(metaclass=SingletonMeta):
    def __init__(self):
        self._topic_id_lookup: dict[str, uuid.UUID] = {}
        self._topic_name_lookup: dict[uuid.UUID, str] = {}

        self._partition_indices_lookup: defaultdict[uuid.UUID, list[int]] = defaultdict(list)

        for record_batch in list(read_record_batches("__cluster_metadata", 0)):
            for record in record_batch.records:
                self._add_record(record)

    def get_topic_id(self, topic_name: str):
        return self._topic_id_lookup.get(topic_name)

    def get_topic_name(self, topic_id: uuid.UUID):
        return self._topic_name_lookup.get(topic_id)

    def get_partition_indices(self, topic_id: uuid.UUID):
        return self._partition_indices_lookup.get(topic_id)

    def _add_record(self, record: Record):
        binary_stream = io.BytesIO(record.value)
        decode_int8(binary_stream)
        match decode_int8(binary_stream):
            case RecordType.TOPIC:
                decode_int8(binary_stream)
                topic_name = decode_compact_string(binary_stream)
                topic_id = decode_uuid(binary_stream)

                self._topic_id_lookup[topic_name] = topic_id
                self._topic_name_lookup[topic_id] = topic_name
            case RecordType.PARTITION:
                decode_int8(binary_stream)
                partition_index = decode_int32(binary_stream)
                topic_id = decode_uuid(binary_stream)

                self._partition_indices_lookup[topic_id].append(partition_index)


def read_record_batches(topic_name: str, partition_index: int):
    filepath = f"/tmp/kraft-combined-logs/{topic_name}-{partition_index}/00000000000000000000.log"
    with io.open(filepath, mode="rb") as binary_stream:
        while binary_stream.peek():
            yield RecordBatch.decode(binary_stream)
