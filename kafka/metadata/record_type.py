import enum


@enum.unique
class RecordType(enum.IntEnum):
    TOPIC = 2
    PARTITION = 3
    FEATURE_LEVEL = 12
