from .api_key import ApiKey
from .request import Request
from .response import *


def request_to_response(request: Request) -> bytes:
    match request.api_key:
        case ApiKey.FETCH:
            response = FetchResponse.from_request(request)
        case ApiKey.API_VERSIONS:
            response = ApiVersionsResponse.from_request(request)
    print(response)

    response_header = request.correlation_id.to_bytes(4)
    response_body = response.serialize()
    response_length = len(response_header) + len(response_body)

    return response_length.to_bytes(4) + response_header + response_body
