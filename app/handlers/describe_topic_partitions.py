__all__ = "handle_describe_topic_partitions_request",

from uuid import UUID

from ..cluster_metadata import ClusterMetadata
from ..protocol.constants import ErrorCode
from ..protocol.requests import DescribeTopicPartitionsRequest
from ..protocol.requests.describe_topic_partitions import TopicRequest
from ..protocol.responses import DescribeTopicPartitionsResponse
from ..protocol.responses.describe_topic_partitions import ResponsePartition, ResponseTopic

from .utils import make_response_header


def _make_response_topic(topic_request: TopicRequest) -> ResponseTopic:
    topic = ClusterMetadata().get_topic(topic_request.name)
    if topic is None:
        return ResponseTopic(
            error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
            name=topic_request.name,
            topic_id=UUID(int=0),
            is_internal=False,
            partitions=[],
            topic_authorized_operations=0,
        )

    return ResponseTopic(
        error_code=ErrorCode.NONE,
        name=topic_request.name,
        topic_id=topic.topic_id,
        is_internal=False,
        partitions=[
            ResponsePartition(
                error_code=ErrorCode.NONE,
                partition_index=p.partition_index,
                leader_id=p.leader_id,
                leader_epoch=p.leader_epoch,
                replica_nodes=p.replicas,
                isr_nodes=p.isr,
                eligible_leader_replicas=None,
                last_known_elr=None,
                offline_replicas=[],
            )
            for p in topic.partitions.values()
        ],
        topic_authorized_operations=0,
    )


def handle_describe_topic_partitions_request(request: DescribeTopicPartitionsRequest) -> DescribeTopicPartitionsResponse:
    return DescribeTopicPartitionsResponse(
        header=make_response_header(request),
        throttle_time_ms=0,
        topics=[_make_response_topic(topic_request) for topic_request in request.topics],
        next_cursor=None,
    )
