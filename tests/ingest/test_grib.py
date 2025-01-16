import pytest
import mock
import tempfile
import os
from collections import defaultdict

from wx_explore.ingest.grib import ingest_grib_file
from wx_explore.common.models import Source, SourceField, Metric
from wx_explore.common import storage

class MockGribMessage:
    def __init__(self, values, projparams=None, validDate=None, analDate=None):
        self.values = values
        self.projparams = projparams or {}
        self.validDate = validDate
        self.analDate = analDate
    
    def valid_key(self, key):
        return False

class MockGribFile:
    def __init__(self, messages):
        self.messages = messages
        self.closed = False
        
    def select(self, **kwargs):
        return self.messages
        
    def close(self):
        self.closed = True

@pytest.fixture
def mock_storage():
    with mock.patch('wx_explore.common.storage.get_provider') as mock_provider:
        provider = mock.MagicMock()
        mock_provider.return_value = provider
        yield provider

@pytest.fixture
def mock_db_session():
    with mock.patch('wx_explore.web.core.db.session') as session:
        yield session

def test_file_handle_cleanup(mock_storage, mock_db_session):
    """Test that file handles are properly closed even when exceptions occur"""
    # Create a temporary file
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # Create mock source and field
        source = Source(id=1, short_name='test')
        field = SourceField(
            source_id=1,
            metric=Metric(intermediate=False),
            selectors={'shortName': 'test'}
        )
        
        # Setup mock query
        mock_query = mock.MagicMock()
        mock_query.filter.return_value.all.return_value = [field]
        SourceField.query = mock_query

        # Create mock grib file that raises an exception
        mock_grib = MockGribFile([])
        mock_grib.select = mock.MagicMock(side_effect=Exception("Test error"))
        
        with mock.patch('pygrib.open', return_value=mock_grib):
            with pytest.raises(Exception):
                ingest_grib_file(temp_path, source)
            
            # Verify file was closed
            assert mock_grib.closed == True
            
    finally:
        os.unlink(temp_path)

def test_memory_usage_large_messages(mock_storage, mock_db_session):
    """Test that memory usage stays bounded when processing large messages"""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # Create mock source and field
        source = Source(id=1, short_name='test')
        field = SourceField(
            source_id=1,
            metric=Metric(intermediate=False),
            selectors={'shortName': 'test'}
        )
        
        # Setup mock query
        mock_query = mock.MagicMock()
        mock_query.filter.return_value.all.return_value = [field]
        SourceField.query = mock_query

        # Create large mock messages (10MB each)
        large_data = [1.0] * (10 * 1024 * 1024)  # 10MB of floats
        messages = [
            MockGribMessage(
                values=large_data.copy(),
                projparams={'proj': 'test'},
                validDate='2023-01-01',
                analDate='2023-01-01'
            )
            for _ in range(10)  # 100MB total
        ]
        
        mock_grib = MockGribFile(messages)
        
        # Track max memory usage
        max_memory = 0
        stored_chunks = []
        
        def mock_put_fields(proj, fields):
            nonlocal max_memory, stored_chunks
            # Calculate approximate memory usage of current data
            current_memory = sum(
                sum(len(msg) * 8 for msg in msgs)  # 8 bytes per float
                for msgs in fields.values()
            )
            max_memory = max(max_memory, current_memory)
            stored_chunks.append(len(fields))
        
        mock_storage.put_fields.side_effect = mock_put_fields
        
        with mock.patch('pygrib.open', return_value=mock_grib):
            ingest_grib_file(temp_path, source)
            
            # Verify memory usage stayed under 50MB at any time
            assert max_memory < 50 * 1024 * 1024, f"Memory usage exceeded 50MB: {max_memory / 1024 / 1024}MB"
            
            # Verify data was stored in multiple chunks
            assert len(stored_chunks) > 1, "Data was not stored incrementally"
            
    finally:
        os.unlink(temp_path)

def test_incremental_storage(mock_storage, mock_db_session):
    """Test that data is stored incrementally rather than all at once"""
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_path = temp_file.name
    
    try:
        # Create mock source and field
        source = Source(id=1, short_name='test')
        field = SourceField(
            source_id=1,
            metric=Metric(intermediate=False),
            selectors={'shortName': 'test'}
        )
        
        # Setup mock query
        mock_query = mock.MagicMock()
        mock_query.filter.return_value.all.return_value = [field]
        SourceField.query = mock_query

        # Create mock messages
        messages = [
            MockGribMessage(
                values=[1.0] * 1000,
                projparams={'proj': 'test'},
                validDate='2023-01-01',
                analDate='2023-01-01'
            )
            for _ in range(5)
        ]
        
        mock_grib = MockGribFile(messages)
        
        # Track storage calls
        storage_calls = []
        
        def mock_put_fields(proj, fields):
            storage_calls.append(len(fields))
        
        mock_storage.put_fields.side_effect = mock_put_fields
        
        with mock.patch('pygrib.open', return_value=mock_grib):
            ingest_grib_file(temp_path, source)
            
            # Verify multiple storage calls were made
            assert len(storage_calls) > 1, "Data was not stored incrementally"
            # Verify each storage call handled a subset of the data
            assert all(call < 5 for call in storage_calls), "Too much data stored at once"
            
    finally:
        os.unlink(temp_path)