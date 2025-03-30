import abc
import dataclasses
import enum
import io
import typing
import uuid

from ..protocol import (
    Readable,
    decode_compact_array,
    decode_compact_string,
    decode_int16,
    decode_int32,
    decode_int8,
    decode_tagged_fields,
    decode_uuid,
    decode_varint,
)


@dataclasses.dataclass(frozen=True)
class Record(abc.ABC):
    @classmethod
    @abc.abstractmethod
    def decode(cls, readable: Readable) -> typing.Self:
        raise NotImplementedError


@dataclasses.dataclass(frozen=True)
class TopicRecord(Record):
    name: str
    topic_id: uuid.UUID

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        record = cls(
            name=decode_compact_string(readable),
            topic_id=decode_uuid(readable),
        )
        decode_tagged_fields(readable)
        return record


@dataclasses.dataclass(frozen=True)
class PartitionRecord(Record):
    partition_id: int
    topic_id: uuid.UUID
    replicas: list[int]
    isr: list[int]
    removing_replicas: list[int]
    adding_replicas: list[int]
    leader: int
    leader_epoch: int
    partition_epoch: int
    directories: list[uuid.UUID]

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        record = cls(
            partition_id=decode_int32(readable),
            topic_id=decode_uuid(readable),
            replicas=decode_compact_array(readable, decode_int32),
            isr=decode_compact_array(readable, decode_int32),
            removing_replicas=decode_compact_array(readable, decode_int32),
            adding_replicas=decode_compact_array(readable, decode_int32),
            leader=decode_int32(readable),
            leader_epoch=decode_int32(readable),
            partition_epoch=decode_int32(readable),
            directories=decode_compact_array(readable, decode_uuid),
        )
        decode_tagged_fields(readable)
        return record


@dataclasses.dataclass(frozen=True)
class FeatureLevelRecord(Record):
    name: str
    feature_level: int

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        record = cls(
            name=decode_compact_string(readable),
            feature_level=decode_int16(readable),
        )
        decode_tagged_fields(readable)
        return record


@enum.unique
class RecordType(enum.IntEnum):
    TOPIC = 2
    PARTITION = 3
    FEATURE_LEVEL = 12

    @classmethod
    def decode(cls, readable: Readable) -> typing.Self:
        return cls(decode_int8(readable))


def decode_record(readable: Readable) -> Record:
    length = decode_varint(readable)
    new_readable = io.BytesIO(readable.read(length))
    attributes = decode_int8(new_readable)
    timestamp_delta = decode_varint(new_readable)
    offset_delta = decode_varint(new_readable)
    key_length = decode_varint(new_readable)
    if key_length >= 0:
        key = new_readable.read(key_length)
    value_length = decode_varint(new_readable)
    new_readable = io.BytesIO(new_readable.read(value_length))

    frame_version = decode_int8(new_readable)
    record_type = RecordType.decode(new_readable)
    version = decode_int8(new_readable)

    match record_type:
        case RecordType.TOPIC:
            return TopicRecord.decode(new_readable)
        case RecordType.PARTITION:
            return PartitionRecord.decode(new_readable)
        case RecordType.FEATURE_LEVEL:
            return FeatureLevelRecord.decode(new_readable)
