__all__ = "handle_produce_request",

from ..cluster_metadata import ClusterMetadata
from ..protocol.constants import ErrorCode
from ..protocol.requests import ProduceRequest
from ..protocol.requests.produce import PartitionProduceData, TopicProduceData
from ..protocol.responses import ProduceResponse
from ..protocol.responses.produce import PartitionProduceResponse, TopicProduceResponse

from .utils import make_response_header


def _is_valid_topic_partition(topic_name: str, partition_index: int) -> bool:
    topic = ClusterMetadata().get_topic(name=topic_name)
    return topic is not None and partition_index in topic.partitions


def _make_partition_produce_response(topic_name: str, partition_data: PartitionProduceData) -> PartitionProduceResponse:
    if not _is_valid_topic_partition(topic_name, partition_data.index):
        return PartitionProduceResponse(
            index=partition_data.index,
            error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
            base_offset=-1,
            log_append_time_ms=-1,
            log_start_offset=-1,
            record_errors=[],
            error_message=None,
        )

    if partition_data.records is not None:
        path = f"/tmp/kraft-combined-logs/{topic_name}-{partition_data.index}/00000000000000000000.log"
        with open(path, mode="ab") as f:
            f.write(partition_data.records)

    return PartitionProduceResponse(
        index=partition_data.index,
        error_code=ErrorCode.NONE,
        base_offset=0,
        log_append_time_ms=-1,
        log_start_offset=0,
        record_errors=[],
        error_message=None,
    )


def _make_topic_produce_response(topic_data: TopicProduceData) -> TopicProduceResponse:
    return TopicProduceResponse(
        name=topic_data.name,
        partition_responses=[
            _make_partition_produce_response(topic_data.name, p) for p in topic_data.partition_data
        ],
    )


def handle_produce_request(request: ProduceRequest) -> ProduceResponse:
    return ProduceResponse(
        header=make_response_header(request),
        responses=[_make_topic_produce_response(t) for t in request.topic_data],
        throttle_time_ms=0,
    )
