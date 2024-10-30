import asyncio
import io

from ..constants import ApiKey

from .api_versions_request import ApiVersionsRequest
from .api_versions_response import ApiVersionsResponse
from .describe_topic_partitions_request import DescribeTopicPartitionsRequest
from .describe_topic_partitions_response import DescribeTopicPartitionsResponse
from .fetch_request import FetchRequest
from .fetch_response import FetchResponse
from .request_header import RequestHeader
from .response_header import ResponseHeader


async def read_request(stream_reader: asyncio.StreamReader):
    n = int.from_bytes(await stream_reader.readexactly(4))
    binary_stream = io.BytesIO(await stream_reader.readexactly(n))

    header = RequestHeader.decode(binary_stream)
    match header.api_key:
        case ApiKey.FETCH:
            request_class = FetchRequest
        case ApiKey.API_VERSIONS:
            request_class = ApiVersionsRequest
        case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
            request_class = DescribeTopicPartitionsRequest

    return request_class(header, **request_class.decode_body_kwargs(binary_stream))


def make_response(request):
    header = ResponseHeader.from_request_header(request.header)
    match request.header.api_key:
        case ApiKey.FETCH:
            response_class = FetchResponse
        case ApiKey.API_VERSIONS:
            response_class = ApiVersionsResponse
        case ApiKey.DESCRIBE_TOPIC_PARTITIONS:
            response_class = DescribeTopicPartitionsResponse

    return response_class(header, **response_class.make_body_kwargs(request))
