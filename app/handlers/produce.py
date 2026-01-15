__all__ = "handle_produce_request",

from ..protocol.constants import ErrorCode
from ..protocol.requests import ProduceRequest
from ..protocol.responses import ProduceResponse
from ..protocol.responses.produce import PartitionProduceResponse, TopicProduceResponse

from .utils import make_response_header


def handle_produce_request(request: ProduceRequest) -> ProduceResponse:
    return ProduceResponse(
        header=make_response_header(request),
        responses=[
            TopicProduceResponse(
                name=request.topic_data[0].name,
                partition_responses=[
                    PartitionProduceResponse(
                        index=request.topic_data[0].partition_data[0].index,
                        error_code=ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        base_offset=-1,
                        log_append_time_ms=-1,
                        log_start_offset=-1,
                        record_errors=[],
                        error_message=None,
                    ),
                ],
            ),
        ],
        throttle_time_ms=0,
    )
