import pytest
from unittest.mock import patch, MagicMock, ANY
import botocore
from sqlalchemy.orm import Session
from wx_explore.cloud.functions.reduce_grib import func
from wx_explore.common.models import Source, SourceField

class TestReduceGrib:
    @pytest.fixture
    def mock_request(self):
        return MagicMock(args={
            'url': 'http://test.com/grib',
            'idx': '0',
            'source_name': 'test_source',
            'out': 'test.grib2'
        })

    @pytest.fixture
    def mock_session(self):
        with patch('sqlalchemy.orm.Session') as mock:
            session = MagicMock(spec=Session)
            mock.return_value = session
            yield session

    @pytest.fixture
    def mock_s3(self):
        with patch('wx_explore.cloud.helpers.s3_client') as mock:
            s3 = MagicMock()
            mock.return_value = s3
            yield s3

    def test_session_cleanup_success(self, mock_request, mock_session, mock_s3):
        """Test that session is properly closed on successful execution"""
        # Setup mock query results
        source_field = MagicMock(spec=SourceField)
        mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [source_field]

        # Execute function
        func(mock_request)

        # Verify session was closed
        mock_session.close.assert_called_once()

    def test_session_cleanup_on_error(self, mock_request, mock_session, mock_s3):
        """Test that session is closed even when an error occurs"""
        # Setup mock to raise an error during S3 upload
        mock_s3.upload_fileobj.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'InternalError', 'Message': 'Test error'}},
            'upload_fileobj'
        )

        # Setup mock query results
        source_field = MagicMock(spec=SourceField)
        mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [source_field]

        # Execute function and verify error handling
        with pytest.raises(botocore.exceptions.ClientError):
            func(mock_request)

        # Verify session was still closed
        mock_session.close.assert_called_once()

    def test_s3_upload_error_handling(self, mock_request, mock_session, mock_s3):
        """Test proper error handling for S3 upload failures"""
        # Setup mock query results
        source_field = MagicMock(spec=SourceField)
        mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [source_field]

        # Setup mock to raise an error during S3 upload
        mock_s3.upload_fileobj.side_effect = botocore.exceptions.ClientError(
            {'Error': {'Code': 'NoSuchBucket', 'Message': 'The bucket does not exist'}},
            'upload_fileobj'
        )

        # Execute function and verify error handling
        with pytest.raises(botocore.exceptions.ClientError) as exc_info:
            func(mock_request)

        assert 'NoSuchBucket' in str(exc_info.value)
        mock_session.close.assert_called_once()

    def test_context_manager_cleanup(self, mock_request, mock_session, mock_s3):
        """Test that temporary file context manager properly cleans up"""
        # Setup mock query results
        source_field = MagicMock(spec=SourceField)
        mock_session.query.return_value.join.return_value.filter.return_value.all.return_value = [source_field]

        with patch('tempfile.TemporaryFile') as mock_temp:
            temp_file = MagicMock()
            mock_temp.return_value.__enter__.return_value = temp_file
            
            func(mock_request)

            # Verify temp file was properly used and cleaned up
            mock_temp.assert_called_once()
            mock_temp.return_value.__exit__.assert_called_once()
            temp_file.seek.assert_called_once_with(0)