import pytest
import boto3
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from sqlalchemy.exc import OperationalError

from wx_explore.cloud.helpers import db_engine

def test_db_engine_aws_auth_success():
    """Test successful AWS IAM authentication"""
    mock_client = MagicMock()
    mock_client.generate_db_auth_token.return_value = "test-token"
    
    with patch('boto3.client', return_value=mock_client):
        with patch('os.environ', {'AWS_REGION': 'us-east-1'}):
            engine = db_engine({'host': 'test-host', 'port': 5432})
            assert engine is not None
            mock_client.generate_db_auth_token.assert_called_once_with(
                DBHostname='test-host',
                Port=5432,
                Region='us-east-1'
            )

def test_db_engine_retry_mechanism():
    """Test retry mechanism with exponential backoff"""
    mock_client = MagicMock()
    mock_client.generate_db_auth_token.side_effect = [
        ClientError({'Error': {'Code': 'ThrottlingException'}}, 'GenerateToken'),
        "test-token"
    ]
    
    with patch('boto3.client', return_value=mock_client):
        with patch('os.environ', {'AWS_REGION': 'us-east-1'}):
            engine = db_engine({'host': 'test-host', 'port': 5432})
            assert engine is not None
            assert mock_client.generate_db_auth_token.call_count == 2

def test_db_engine_auth_failure():
    """Test proper exception handling for auth failures"""
    mock_client = MagicMock()
    mock_client.generate_db_auth_token.side_effect = ClientError(
        {'Error': {'Code': 'InvalidClientTokenId'}}, 'GenerateToken'
    )
    
    with patch('boto3.client', return_value=mock_client):
        with patch('os.environ', {'AWS_REGION': 'us-east-1'}):
            with pytest.raises(OperationalError) as exc_info:
                db_engine({'host': 'test-host', 'port': 5432})
            assert "Failed to authenticate with AWS IAM" in str(exc_info.value)

def test_db_engine_max_retries_exceeded():
    """Test behavior when max retries are exceeded"""
    mock_client = MagicMock()
    mock_client.generate_db_auth_token.side_effect = ClientError(
        {'Error': {'Code': 'ThrottlingException'}}, 'GenerateToken'
    )
    
    with patch('boto3.client', return_value=mock_client):
        with patch('os.environ', {'AWS_REGION': 'us-east-1'}):
            with pytest.raises(OperationalError) as exc_info:
                db_engine({'host': 'test-host', 'port': 5432})
            assert "Max retries exceeded" in str(exc_info.value)