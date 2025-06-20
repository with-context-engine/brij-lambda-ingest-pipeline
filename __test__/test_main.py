import json
import os
import tempfile
from unittest.mock import Mock, patch, MagicMock, call
import pytest
from PIL import Image
import io

# Import the functions we want to test
from ingest_pipeline.main import (
    lambda_handler,
    get_next_task_sequence,
    create_and_upload_task,
    process_pdf_page,
    process_pdf,
    process_png,
    move_original_to_raw,
    process_s3_record
)


class TestLambdaHandler:
    """Test suite for the Lambda handler and its functions."""
    
    @pytest.fixture
    def sample_sqs_event(self):
        """Sample SQS event with S3 notification."""
        return {
            "Records": [{
                "messageId": "42e2d2ed-65de-42db-bfaa-9d04ee397dc5",
                "receiptHandle": "AQEBZah8dAcvxL/kRUO9KKQQMwi+njiqWLi4EJP5tdcQ3FrJVwo/Yf5q8gYieTu+diOcAeLZSrC2ig/BWPYi2oz1EyqlaZRvDnjURVzC7H+cAP3WqXQ34VxAPk7Q5IWTorNb+d6z7BYvdhu2aCuVPXMDkrINRAMSCbuGu7H562YXau+0GIQhqWDpZUyX5uX34DBx6RLAl5+At/5S9qwb1UL6XLYoXweIv1yJ5W3yRVq3Vlygtp1HRCq+zKRiG1ORy67NMB52+Bwf4CKdt6OMRL0uKSg+FE9oKwIfKj/PuIG3q4lkyan1Icy9w1anBcTo6U8mQxzQsDEba36eDUCVEA7NBvSifI4GOV0T+0Mz+HSE0kpDgx0CqxtyHGFStkBfEgMtzlHEcE2plqoG28AUeG9gjg==",
                "body": json.dumps({
                    "Records": [{
                        "eventVersion": "2.1",
                        "eventSource": "aws:s3",
                        "awsRegion": "us-east-1",
                        "eventTime": "2025-06-19T22:53:18.720Z",
                        "eventName": "ObjectCreated:Put",
                        "userIdentity": {"principalId": "AWS:AIDATGW6D4T35VO3LM4FT"},
                        "requestParameters": {"sourceIPAddress": "96.250.79.68"},
                        "responseElements": {
                            "x-amz-request-id": "P5GBR9KGYW6HZ3MA",
                            "x-amz-id-2": "IbTG1f+r0tBMobjHdn4j2izKhusX9eBaCx7N8RbIGx7x23e9SAcC2bSCNNoVA8jV24Z7blJ17+j6QYuP8jlJNn6528sfQVbS"
                        },
                        "s3": {
                            "s3SchemaVersion": "1.0",
                            "configurationId": "tf-s3-queue-20250619224812800500000002",
                            "bucket": {
                                "name": "brij-v1-bucket",
                                "ownerIdentity": {"principalId": "AN6L0W7Z1W8K6"},
                                "arn": "arn:aws:s3:::brij-v1-bucket"
                            },
                            "object": {
                                "key": "upload/BD23-25943+FLR+PLN.pdf",
                                "size": 435027,
                                "eTag": "80955dc9417677628d46674340d1acc9",
                                "versionId": "0RqUcc4jvul.yempoufM1IBRyWt0sNWt",
                                "sequencer": "00685494DE8E151A1B"
                            }
                        }
                    }]
                }),
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1750373599461",
                    "SenderId": "AROA4R74ZO52XAB5OD7T4:S3-PROD-END",
                    "ApproximateFirstReceiveTimestamp": "1750373599472"
                },
                "messageAttributes": {},
                "md5OfBody": "7d84fe3663343107b186e691b40080ac",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-1:220582896887:brij-v1-upload-queue",
                "awsRegion": "us-east-1"
            }]
        }
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client."""
        with patch('ingest_pipeline.main.s3') as mock_s3:
            yield mock_s3
    
    def test_get_next_task_sequence_empty(self, mock_s3_client):
        """Test getting next sequence when no tasks exist."""
        mock_s3_client.list_objects_v2.return_value = {}
        
        seq = get_next_task_sequence("test-bucket")
        
        assert seq == 1
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="test-bucket", 
            Prefix="ingest/"
        )
    
    def test_get_next_task_sequence_with_existing(self, mock_s3_client):
        """Test getting next sequence with existing tasks."""
        mock_s3_client.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "ingest/TASK_0000001.json"},
                {"Key": "ingest/TASK_0000003.json"},
                {"Key": "ingest/TASK_0000002.json"},
            ]
        }
        
        seq = get_next_task_sequence("test-bucket")
        
        assert seq == 4
    
    def test_create_and_upload_task(self, mock_s3_client):
        """Test creating and uploading a task."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            ingest_key = create_and_upload_task(
                "test-bucket",
                "s3://test-bucket/raw/test.png",
                5,
                tmp_dir
            )
            
            assert ingest_key == "ingest/TASK_0000005.json"
            mock_s3_client.upload_file.assert_called_once()
            
            # Verify the JSON file was created correctly
            call_args = mock_s3_client.upload_file.call_args
            local_file = call_args[0][0]
            assert os.path.exists(local_file)
            
            with open(local_file, 'r') as f:
                content = json.load(f)
                assert content == [{"data": {"image": "s3://test-bucket/raw/test.png"}}]
    
    @patch('ingest_pipeline.main.get_next_task_sequence')
    @patch('ingest_pipeline.main.create_and_upload_task')
    def test_process_pdf_page(self, mock_create_task, mock_get_seq, mock_s3_client):
        """Test processing a single PDF page."""
        mock_get_seq.return_value = 1
        mock_create_task.return_value = "ingest/TASK_0000001.json"
        
        # Create a mock PyMuPDF page
        mock_page = Mock()
        mock_pixmap = Mock()
        mock_page.get_pixmap.return_value = mock_pixmap
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_key = process_pdf_page(
                mock_page,
                1,
                "test_document",
                "test-bucket",
                tmp_dir
            )
            
            assert raw_key == "raw/test_document_0001.png"
            mock_page.get_pixmap.assert_called_once()
            mock_pixmap.save.assert_called_once()
            mock_s3_client.upload_file.assert_called_once()
    
    @patch('ingest_pipeline.main.fitz')
    @patch('ingest_pipeline.main.process_pdf_page')
    def test_process_pdf_multiple_pages(self, mock_process_page, mock_fitz):
        """Test processing a PDF with multiple pages."""
        # Mock PyMuPDF document
        mock_doc = Mock()
        mock_doc.page_count = 2
        mock_doc.__getitem__.side_effect = [Mock(), Mock()]  # 2 pages
        mock_fitz.open.return_value = mock_doc
        
        mock_process_page.side_effect = [
            "raw/doc_0001.png",
            "raw/doc_0002.png",
        ]
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            keys = process_pdf("test.pdf", "doc", "test-bucket", tmp_dir)
            
            assert len(keys) == 2
            assert mock_process_page.call_count == 2
            mock_doc.close.assert_called_once()
    
    @patch('ingest_pipeline.main.get_next_task_sequence')
    @patch('ingest_pipeline.main.create_and_upload_task')
    def test_process_png(self, mock_create_task, mock_get_seq, mock_s3_client):
        """Test processing a PNG file."""
        mock_get_seq.return_value = 1
        mock_create_task.return_value = "ingest/TASK_0000001.json"
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create a mock PNG file
            test_png = os.path.join(tmp_dir, "input.png")
            with open(test_png, 'wb') as f:
                f.write(b'fake png data')
            
            keys = process_png(test_png, "test_image", "test-bucket", tmp_dir)
            
            assert keys == ["raw/test_image.png"]
            mock_s3_client.upload_file.assert_called_once()
    
    def test_move_original_to_raw(self, mock_s3_client):
        """Test moving original file to raw prefix."""
        raw_key = move_original_to_raw(
            "local_file.png",
            "test-bucket",
            "upload/original.png",
            "original",
            ".png"
        )
        
        assert raw_key == "raw/original.png"
        mock_s3_client.upload_file.assert_called_once_with(
            "local_file.png",
            "test-bucket",
            "raw/original.png"
        )
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="upload/original.png"
        )
    
    @patch('ingest_pipeline.main.process_pdf')
    @patch('tempfile.TemporaryDirectory')
    def test_process_s3_record_pdf(self, mock_tempdir, mock_process_pdf, mock_s3_client):
        """Test processing an S3 record for a PDF file."""
        # Setup mocks
        mock_temp = MagicMock()
        mock_temp.__enter__.return_value = "/tmp/test"
        mock_tempdir.return_value = mock_temp
        
        record = {
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "upload/test.pdf"}
                    }
                }]
            })
        }
        
        process_s3_record(record)
        
        mock_s3_client.download_file.assert_called_once()
        # PDFs should not be moved anymore
        mock_s3_client.delete_object.assert_not_called()
        mock_process_pdf.assert_called_once()
    
    def test_process_s3_record_skip_non_upload(self, mock_s3_client):
        """Test skipping files not in upload/ prefix."""
        record = {
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "other/test.pdf"}
                    }
                }]
            })
        }
        
        process_s3_record(record)
        
        # Should not download or process
        mock_s3_client.download_file.assert_not_called()
    
    def test_process_s3_record_skip_unsupported_type(self, mock_s3_client):
        """Test skipping unsupported file types."""
        record = {
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "upload/test.jpg"}
                    }
                }]
            })
        }
        
        process_s3_record(record)
        
        # Should not download or process
        mock_s3_client.download_file.assert_not_called()
    
    @patch('ingest_pipeline.main.process_s3_record')
    def test_lambda_handler_success(self, mock_process_record, sample_sqs_event):
        """Test successful Lambda handler execution."""
        result = lambda_handler(sample_sqs_event, None)
        
        assert result == {"status": "success"}
        mock_process_record.assert_called_once()
    
    @patch('ingest_pipeline.main.process_s3_record')
    def test_lambda_handler_error_handling(self, mock_process_record, sample_sqs_event):
        """Test Lambda handler error handling."""
        mock_process_record.side_effect = Exception("Test error")
        
        # Should not raise exception, but handle it
        result = lambda_handler(sample_sqs_event, None)
        
        assert result == {"status": "success"}
    
    def test_url_decode_in_process_s3_record(self, mock_s3_client):
        """Test that URL-encoded keys are properly decoded."""
        record = {
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "upload/file+with+spaces.pdf"}
                    }
                }]
            })
        }
        
        with patch('ingest_pipeline.main.process_pdf'):
            with patch('tempfile.TemporaryDirectory'):
                process_s3_record(record)
        
        # Check that download was called with decoded key
        mock_s3_client.download_file.assert_called_once()
        call_args = mock_s3_client.download_file.call_args
        assert call_args[0][1] == "upload/file with spaces.pdf"


class TestIntegration:
    """Integration tests that test the full flow."""
    
    @patch('ingest_pipeline.main.s3')
    @patch('ingest_pipeline.main.fitz')
    def test_full_pdf_processing_flow(self, mock_fitz, mock_s3):
        """Test the complete flow of processing a PDF file."""
        # Setup mocks
        mock_s3.list_objects_v2.return_value = {}
        
        # Create mock PyMuPDF document with 2 pages
        mock_doc = Mock()
        mock_doc.page_count = 2
        mock_page1 = Mock()
        mock_page2 = Mock()
        mock_pixmap1 = Mock()
        mock_pixmap2 = Mock()
        mock_page1.get_pixmap.return_value = mock_pixmap1
        mock_page2.get_pixmap.return_value = mock_pixmap2
        mock_doc.__getitem__.side_effect = [mock_page1, mock_page2]
        mock_fitz.open.return_value = mock_doc
        
        # Create test event
        event = {
            "Records": [{
                "body": json.dumps({
                    "Records": [{
                        "s3": {
                            "bucket": {"name": "test-bucket"},
                            "object": {"key": "upload/test_document.pdf"}
                        }
                    }]
                })
            }]
        }
        
        # Execute
        result = lambda_handler(event, None)
        
        # Verify
        assert result == {"status": "success"}
        
        # Should have downloaded the original
        assert mock_s3.download_file.call_count == 1
        
        # Should have uploaded: 2 PNGs + 2 JSONs = 4 files (PDF not moved anymore)
        assert mock_s3.upload_file.call_count == 4
        
        # Should NOT have deleted the original PDF
        assert mock_s3.delete_object.call_count == 0


class TestRealPDFProcessing:
    """Test with real PDF file and verify PNG output."""
    
    @pytest.fixture
    def test_pdf_path(self):
        """Path to the test PDF file."""
        return os.path.join(os.path.dirname(__file__), "artifacts", "test_pdf.pdf")
    
    @patch('ingest_pipeline.main.s3')
    def test_process_pdf_real_file_png_quality(self, mock_s3, test_pdf_path):
        """Test processing a real PDF and verify PNG output quality."""
        # Setup mock
        mock_s3.list_objects_v2.return_value = {}
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Process the PDF (using mocked PyMuPDF)
            processed_keys = process_pdf(test_pdf_path, "test_pdf", "test-bucket", tmp_dir)
            
            # Verify that PNGs were created
            assert len(processed_keys) == 2
            
            # Check each PNG file
            png_sizes = []
            for i, key in enumerate(processed_keys):
                # Expected filename format: raw/test_pdf_0001.png, raw/test_pdf_0002.png, etc.
                expected_key = f"raw/test_pdf_{i+1:04d}.png"
                assert key == expected_key
                
                # Find the local PNG file
                local_png = os.path.join(tmp_dir, f"test_pdf_{i+1:04d}.png")
                
                # Verify the file exists and has reasonable size
                assert os.path.exists(local_png)
                file_size = os.path.getsize(local_png)
                png_sizes.append(file_size)
                
                # Ensure PNG is not suspiciously small
                assert file_size > 1_000_000, f"PNG file {local_png} is too small: {file_size} bytes"
                
                # Verify it starts with PNG header
                with open(local_png, 'rb') as f:
                    header = f.read(8)
                    assert header.startswith(b'\x89PNG\r\n\x1a\n'), f"Not a valid PNG file: {local_png}"
            
            # Verify file sizes are reasonable
            print(f"PNG file sizes: {png_sizes}")
            assert len(png_sizes) == 2, "Should generate 2 PNG files"
    
    @patch('ingest_pipeline.main.s3')
    def test_process_pdf_page_format(self, mock_s3):
        """Test that PDF pages are named with the correct format."""
        mock_s3.list_objects_v2.return_value = {}
        
        # Create a mock PyMuPDF page
        mock_page = Mock()
        mock_pixmap = Mock()
        mock_page.get_pixmap.return_value = mock_pixmap
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Process page 1
            key1 = process_pdf_page(mock_page, 1, "test_doc", "test-bucket", tmp_dir)
            assert key1 == "raw/test_doc_0001.png"
            
            # Process page 10  
            key10 = process_pdf_page(mock_page, 10, "test_doc", "test-bucket", tmp_dir)
            assert key10 == "raw/test_doc_0010.png"
            
            # Process page 100
            key100 = process_pdf_page(mock_page, 100, "test_doc", "test-bucket", tmp_dir)
            assert key100 == "raw/test_doc_0100.png"
    
    @pytest.fixture
    def mock_s3_client(self):
        """Mock S3 client."""
        with patch('ingest_pipeline.main.s3') as mock_s3:
            yield mock_s3
    
    def test_pdf_not_moved_to_raw(self, mock_s3_client):
        """Test that PDF files are NOT moved from upload/ to raw/."""
        record = {
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "upload/test.pdf"}
                    }
                }]
            })
        }
        
        with patch('ingest_pipeline.main.process_pdf'):
            with patch('tempfile.TemporaryDirectory'):
                process_s3_record(record)
        
        # Should download the file
        mock_s3_client.download_file.assert_called_once()
        
        # Should NOT delete the original PDF
        mock_s3_client.delete_object.assert_not_called()
    
    def test_png_still_moved_to_raw(self, mock_s3_client):
        """Test that PNG files are moved from upload/ to raw/."""
        record = {
            "body": json.dumps({
                "Records": [{
                    "s3": {
                        "bucket": {"name": "test-bucket"},
                        "object": {"key": "upload/test.png"}
                    }
                }]
            })
        }
        
        with patch('ingest_pipeline.main.process_png'):
            with patch('ingest_pipeline.main.move_original_to_raw') as mock_move:
                with patch('tempfile.TemporaryDirectory'):
                    process_s3_record(record)
        
        # Should call move_original_to_raw for PNG files
        mock_move.assert_called_once() 