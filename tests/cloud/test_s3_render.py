import unittest
from unittest.mock import patch, Mock, MagicMock
import io
import numpy as np
from PIL import Image
from wx_explore.cloud.functions.s3_render import func, HttpRequest, HttpResponse, ColorMapEntry

class TestS3Render(unittest.TestCase):
    def setUp(self):
        # Create sample test data
        self.test_data = np.zeros((10, 10), dtype=np.int16)
        self.mock_msg = Mock()
        self.mock_msg.data.return_value = (self.test_data, None)
        
        # Sample valid color map
        self.valid_cm = "0,0,0,0;100,255,255,255"
        
    @patch('wx_explore.cloud.functions.s3_render.requests.get')
    @patch('wx_explore.cloud.functions.s3_render.pygrib.open')
    def test_successful_render(self, mock_pygrib_open, mock_requests_get):
        """Test successful image rendering with valid S3 path"""
        # Setup mocks
        mock_response = Mock()
        mock_response.content = b"mock_grib_data"
        mock_requests_get.return_value.__enter__.return_value = mock_response
        
        mock_grib = Mock()
        mock_grib.read.return_value = [self.mock_msg]
        mock_pygrib_open.return_value = mock_grib
        
        # Make request
        request = HttpRequest(args={'s3_path': 'http://test-bucket/test.grib'})
        response = func(request)
        
        # Verify response
        self.assertEqual(response.code, 200)
        self.assertEqual(response.headers.get('Content-Type'), 'image/png')
        self.assertIsInstance(response.body, bytes)
        
        # Verify cleanup
        mock_grib.close.assert_called_once()

    @patch('wx_explore.cloud.functions.s3_render.requests.get')
    def test_invalid_s3_path(self, mock_requests_get):
        """Test error handling for invalid S3 path"""
        # Setup mock to raise connection error
        mock_requests_get.return_value.__enter__.side_effect = ConnectionError("Failed to connect")
        
        request = HttpRequest(args={'s3_path': 'http://invalid-bucket/test.grib'})
        response = func(request)
        
        self.assertEqual(response.code, 500)
        self.assertIn('Failed to fetch', str(response.body))

    @patch('wx_explore.cloud.functions.s3_render.requests.get')
    def test_request_timeout(self, mock_requests_get):
        """Test timeout handling for S3 requests"""
        # Setup mock to raise timeout
        mock_requests_get.return_value.__enter__.side_effect = requests.exceptions.Timeout("Request timed out")
        
        request = HttpRequest(args={'s3_path': 'http://test-bucket/test.grib'})
        response = func(request)
        
        self.assertEqual(response.code, 500)
        self.assertIn('Request timed out', str(response.body))

    def test_missing_params(self):
        """Test error handling for missing parameters"""
        request = HttpRequest(args={})
        response = func(request)
        
        self.assertEqual(response.code, 400)
        self.assertEqual(response.body, "Missing params")

    @patch('wx_explore.cloud.functions.s3_render.requests.get')
    @patch('wx_explore.cloud.functions.s3_render.pygrib.open')
    def test_colormap_handling(self, mock_pygrib_open, mock_requests_get):
        """Test color map validation and application"""
        # Setup mocks
        mock_response = Mock()
        mock_response.content = b"mock_grib_data"
        mock_requests_get.return_value.__enter__.return_value = mock_response
        
        mock_grib = Mock()
        mock_grib.read.return_value = [self.mock_msg]
        mock_pygrib_open.return_value = mock_grib
        
        # Test invalid color map format
        request = HttpRequest(args={
            's3_path': 'http://test-bucket/test.grib',
            'cm': 'invalid,colormap,format'
        })
        response = func(request)
        
        self.assertEqual(response.code, 400)
        self.assertEqual(response.body, "Malformed color map")
        
        # Test valid color map
        request = HttpRequest(args={
            's3_path': 'http://test-bucket/test.grib',
            'cm': self.valid_cm
        })
        response = func(request)
        
        self.assertEqual(response.code, 200)
        self.assertEqual(response.headers.get('Content-Type'), 'image/png')

if __name__ == '__main__':
    unittest.main()