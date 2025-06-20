# Migration Summary: TIFF to PNG with PyMuPDF

## Overview

Successfully migrated the ingest pipeline from:
- **TIFF output using pdf2image + Pillow** → **PNG output using PyMuPDF**
- **Docker container deployment** → **Zip file deployment with Lambda layers**

## Key Benefits

1. **Simplified Dependencies**: No more Poppler system dependencies
2. **Better Performance**: PyMuPDF is faster than pdf2image for PDF processing
3. **Smaller Package Size**: No Docker container needed
4. **PNG Format**: Web-friendly format for Label Studio
5. **Higher Quality**: 2x scaling for better annotation resolution

## File Changes

### 1. `src/ingest_pipeline/main.py`
- **Removed**: `pdf2image`, `Pillow` imports
- **Added**: `fitz` (PyMuPDF) import
- **Changed**: `process_tiff()` → `process_png()`
- **Updated**: All file extensions from `.tiff` to `.png` 
- **Modified**: PDF processing uses PyMuPDF's `get_pixmap()` instead of `convert_from_path()`
- **Enhanced**: 2x scaling matrix for higher quality output

### 2. `requirements.txt`
- **Removed**: `pdf2image`, `Pillow`
- **Added**: `PyMuPDF`
- **Kept**: `boto3`, `pytest`

### 3. `terraform.tf`
- **Removed**: `lambda_image_uri` variable
- **Removed**: Docker/ECR related configuration
- **Added**: `data.archive_file.lambda_zip` for zip package creation
- **Added**: `aws_lambda_layer_version.pymupdf_layer` for PyMuPDF
- **Changed**: Lambda from `package_type = "Image"` to zip-based deployment
- **Simplified**: Single Lambda function instead of image-based approach

### 4. `Dockerfile`
- **Status**: DELETED (no longer needed)

### 5. `__test__/test_main.py`
- **Updated**: All test assertions from TIFF to PNG
- **Changed**: Mock from `convert_from_path` to PyMuPDF `fitz.open()`
- **Modified**: Test function names: `test_process_tiff` → `test_process_png` 
- **Updated**: File extension assertions throughout
- **Enhanced**: PyMuPDF-specific mocking for `get_pixmap()` and `save()`

### 6. `README.md`
- **Rewritten**: Complete documentation update
- **Added**: Deployment instructions for new architecture
- **Updated**: Feature descriptions for PNG output
- **Added**: Configuration and monitoring sections

## New Files Created

### 7. `scripts/build_layer.sh`
- **Purpose**: Builds PyMuPDF Lambda layer
- **Function**: Creates `pymupdf_layer.zip` for Terraform deployment
- **Features**: Temporary directory cleanup, size reporting

### 8. `scripts/deploy.sh`
- **Purpose**: Complete deployment automation
- **Function**: Builds layer + runs Terraform
- **Features**: Interactive confirmation, step-by-step logging

## Deployment Process Changes

### Before (Docker-based)
1. Build Docker image
2. Push to ECR
3. Reference ECR URI in Terraform variable
4. Deploy with `terraform apply`

### After (Zip-based)
1. Run `./scripts/build_layer.sh` (creates PyMuPDF layer)
2. Run `terraform apply` (or use `./scripts/deploy.sh`)
3. Lambda automatically uses zip + layer

## Image Quality Improvements

### Before (TIFF)
- Standard DPI (300)
- TIFF compression
- Larger file sizes

### After (PNG)
- 2x scaling for higher resolution
- PNG lossless compression  
- Web-friendly format
- Better Label Studio compatibility

## File Processing Changes

### PDF Processing
- **Before**: `convert_from_path()` → `PIL.Image.save()` as TIFF
- **After**: `fitz.open()` → `page.get_pixmap()` → `pix.save()` as PNG
- **Naming**: `document_0001.png`, `document_0002.png`, etc.

### Image Processing  
- **Before**: Process TIFF files, normalize with PIL
- **After**: Process PNG files, simple file copy (no processing needed)

### Storage Behavior
- **PDFs**: Remain in `upload/` folder (unchanged)
- **PNGs**: Moved from `upload/` to `raw/` (was TIFF behavior)

## Testing Updates

All tests updated to reflect:
- PNG file extensions
- PyMuPDF mocking instead of pdf2image
- New function names and parameters
- Updated assertion values

## Backwards Compatibility

⚠️ **Breaking Changes**:
- Output format changed from TIFF to PNG
- Different deployment method (zip vs Docker)
- PyMuPDF dependency instead of pdf2image

## Performance Expectations

- **Faster PDF processing**: PyMuPDF is generally faster than pdf2image
- **Smaller deployment package**: No Docker overhead
- **Better cold start**: Lambda layers reduce initialization time
- **Higher quality output**: 2x scaling improves annotation accuracy

## Verification Steps

To verify the migration:
1. Deploy using `./scripts/deploy.sh`
2. Upload a test PDF to `s3://brij-v1-bucket/upload/`
3. Verify PNG files appear in `s3://brij-v1-bucket/raw/`
4. Check Label Studio tasks in `s3://brij-v1-bucket/ingest/`
5. Confirm PNG quality is suitable for annotation