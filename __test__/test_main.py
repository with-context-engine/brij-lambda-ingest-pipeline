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
    process_tiff,
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
                "s3://test-bucket/raw/test.tiff",
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
                assert content == [{"data": {"image": "s3://test-bucket/raw/test.tiff"}}]
    
    @patch('ingest_pipeline.main.get_next_task_sequence')
    @patch('ingest_pipeline.main.create_and_upload_task')
    def test_process_pdf_page(self, mock_create_task, mock_get_seq, mock_s3_client):
        """Test processing a single PDF page."""
        mock_get_seq.return_value = 1
        mock_create_task.return_value = "ingest/TASK_0000001.json"
        
        # Create a mock PIL Image
        mock_page = Mock()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            raw_key = process_pdf_page(
                mock_page,
                1,
                "test_document",
                "test-bucket",
                tmp_dir
            )
            
            assert raw_key == "raw/test_document_page001.tiff"
            mock_page.save.assert_called_once()
            mock_s3_client.upload_file.assert_called_once()
    
    @patch('ingest_pipeline.main.convert_from_path')
    @patch('ingest_pipeline.main.process_pdf_page')
    def test_process_pdf_multiple_pages(self, mock_process_page, mock_convert):
        """Test processing a PDF with multiple pages."""
        # Mock 3 pages
        mock_pages = [Mock() for _ in range(3)]
        mock_convert.return_value = mock_pages
        mock_process_page.side_effect = [
            "raw/doc_page001.tiff",
            "raw/doc_page002.tiff",
            "raw/doc_page003.tiff"
        ]
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            keys = process_pdf("test.pdf", "doc", "test-bucket", tmp_dir)
            
            assert len(keys) == 3
            assert mock_process_page.call_count == 3
    
    @patch('ingest_pipeline.main.Image')
    @patch('ingest_pipeline.main.get_next_task_sequence')
    @patch('ingest_pipeline.main.create_and_upload_task')
    def test_process_tiff(self, mock_create_task, mock_get_seq, mock_image, mock_s3_client):
        """Test processing a TIFF file."""
        mock_get_seq.return_value = 1
        mock_create_task.return_value = "ingest/TASK_0000001.json"
        mock_img = Mock()
        mock_image.open.return_value = mock_img
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            keys = process_tiff("test.tif", "test_image", "test-bucket", tmp_dir)
            
            assert keys == ["raw/test_image.tiff"]
            mock_img.save.assert_called_once()
            mock_s3_client.upload_file.assert_called_once()
    
    def test_move_original_to_raw(self, mock_s3_client):
        """Test moving original file to raw prefix."""
        raw_key = move_original_to_raw(
            "local_file.pdf",
            "test-bucket",
            "upload/original.pdf",
            "original",
            ".pdf"
        )
        
        assert raw_key == "raw/original.pdf"
        mock_s3_client.upload_file.assert_called_once_with(
            "local_file.pdf",
            "test-bucket",
            "raw/original.pdf"
        )
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="upload/original.pdf"
        )
    
    @patch('ingest_pipeline.main.process_pdf')
    @patch('ingest_pipeline.main.move_original_to_raw')
    @patch('tempfile.TemporaryDirectory')
    def test_process_s3_record_pdf(self, mock_tempdir, mock_move, mock_process_pdf, mock_s3_client):
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
        mock_move.assert_called_once()
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
            with patch('ingest_pipeline.main.move_original_to_raw'):
                with patch('tempfile.TemporaryDirectory'):
                    process_s3_record(record)
        
        # Check that download was called with decoded key
        mock_s3_client.download_file.assert_called_once()
        call_args = mock_s3_client.download_file.call_args
        assert call_args[0][1] == "upload/file with spaces.pdf"


class TestIntegration:
    """Integration tests that test the full flow."""
    
    @patch('ingest_pipeline.main.s3')
    @patch('ingest_pipeline.main.convert_from_path')
    def test_full_pdf_processing_flow(self, mock_convert, mock_s3):
        """Test the complete flow of processing a PDF file."""
        # Setup mocks
        mock_s3.list_objects_v2.return_value = {}
        
        # Create mock pages
        mock_page1 = Mock()
        mock_page2 = Mock()
        mock_convert.return_value = [mock_page1, mock_page2]
        
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
        
        # Should have uploaded: original + 2 TIFFs + 2 JSONs = 5 files
        assert mock_s3.upload_file.call_count == 5
        
        # Should have deleted the original
        assert mock_s3.delete_object.call_count == 1 