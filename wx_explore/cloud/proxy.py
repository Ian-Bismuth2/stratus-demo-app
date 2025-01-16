"""
This is a small shim wrapping the actual functions to make them (in theory) cloud/FaaS provider
agnostic.
"""
from typing import Mapping, Optional, Any

import base64
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)


class HttpRequest(object):
    method: Optional[str]
    path: str
    args: Mapping[str, str]
    headers: Optional[Mapping[str, str]]
    body: Optional[bytes]

    def __init__(self, path, args, method=None, headers=None, body=None):
        self.path = path
        self.args = args
        self.method = method
        self.headers = headers
        self.body = body


class HttpResponse(object):
    code: int
    headers: Mapping[str, str]
    body: bytes

    def __init__(self, body, code=200, headers=None):
        if type(body) is not bytes:
            body = bytes(str(body), 'utf-8')

        self.body = body
        self.code = code
        self.headers = headers if headers is not None else {}

    @staticmethod
    def safe_error_response():
        """Creates a sanitized 500 error response that doesn't leak implementation details."""
        return HttpResponse(
            body=b'{"error": "Internal Server Error"}',
            code=500,
            headers={"Content-Type": "application/json"}
        )


def proxy(cb):
    def inner(*args):
        if 'WEBSITE_SITE_NAME' in os.environ:
            # Azure
            import azure.functions
            az_req: azure.functions.HttpRequest = args[0]
            try:
                req = HttpRequest(
                    method=az_req.method,
                    path='',
                    args=az_req.params,
                    headers=az_req.headers,
                    body=az_req.get_body(),
                )

                resp = cb(req)

                return azure.functions.HttpResponse(
                    status_code=resp.code,
                    headers=resp.headers,
                    body=resp.body,
                )
            except Exception as e:
                logging.error(f"Azure function error: {str(e)}", exc_info=True)
                error_resp = HttpResponse.safe_error_response()
                return azure.functions.HttpResponse(
                    status_code=error_resp.code,
                    headers=error_resp.headers,
                    body=error_resp.body,
                )

        elif 'AWS_LAMBDA_FUNCTION_NAME' in os.environ:
            # AWS Lambda (proxy function)
            try:
                event: Mapping[str, Any] = args[0]
                body = event.get('body', b'')
                if event['isBase64Encoded']:
                    body = base64.b64decode(body)

                req = HttpRequest(
                    method=event['requestContext']['http']['method'],
                    path=event['requestContext']['http']['path'],
                    args=event.get('queryStringParameters', {}),
                    headers=event.get('headers', {}),
                    body=body,
                )

                resp = cb(req)

                return {
                    'statusCode': resp.code,
                    'headers': resp.headers,
                    'body': base64.b64encode(resp.body).decode('ascii'),
                    'isBase64Encoded': True,
                }
            except Exception as e:
                logging.error(f"AWS Lambda function error: {str(e)}", exc_info=True)
                error_resp = HttpResponse.safe_error_response()
                return {
                    'statusCode': error_resp.code,
                    'headers': error_resp.headers,
                    'body': base64.b64encode(error_resp.body).decode('ascii'),
                    'isBase64Encoded': True,
                }

        elif '__OW_ACTION_NAME' in os.environ:
            # OpenWhisk (IBM cloud functions)
            try:
                params = args[0]

                req = HttpRequest(
                    method=params.__ow_method,
                    path='',
                    args=params,
                    headers=params.__ow_headers,
                    # TODO: body
                )

                resp = cb(req)

                return {
                    'statusCode': resp.code,
                    'headers': resp.headers,
                    'body': resp.body,
                    # TODO: base64 if necessary based on content type
                }
            except Exception as e:
                logging.error(f"OpenWhisk function error: {str(e)}", exc_info=True)
                error_resp = HttpResponse.safe_error_response()
                return {
                    'statusCode': error_resp.code,
                    'headers': error_resp.headers,
                    'body': error_resp.body,
                }

        elif 'FUNCTION_NAME' in os.environ:
            # Google Cloud Functions
            try:
                import flask
                gcf_req: flask.Request = args[0]

                req = HttpRequest(
                    method=gcf_req.method,
                    path='',
                    args=gcf_req.args,
                    headers=gcf_req.headers,
                    body=gcf_req.data,
                )

                resp = cb(req)

                return (resp.body, resp.code, resp.headers)
            except Exception as e:
                logging.error(f"Google Cloud function error: {str(e)}", exc_info=True)
                error_resp = HttpResponse.safe_error_response()
                return (error_resp.body, error_resp.code, error_resp.headers)

    return inner