# OCR Module Documentation

This module provides an abstraction layer for OCR (Optical Character Recognition) that supports multiple providers including Tesseract and EasyOCR.

## Features

- **Multiple Provider Support**: Currently supports Tesseract and EasyOCR
- **Unified Interface**: Common API for all OCR providers
- **Flexible Input**: Accepts both PIL Image objects and image bytes
- **Error Handling**: Robust error handling with logging
- **Easy Integration**: Drop-in replacement for existing OCR code

## Installation

### Required Dependencies

For Tesseract:
```bash
pip install pytesseract
# Also install tesseract-ocr system package
# Ubuntu/Debian: sudo apt-get install tesseract-ocr
# macOS: brew install tesseract
```

For EasyOCR:
```bash
pip install easyocr
```

## Usage

### Basic Usage

```python
from ocr import create_ocr

# Create OCR instance with Tesseract
tesseract_ocr = create_ocr(provider="tesseract", lang="eng")

# Create OCR instance with EasyOCR
easyocr_ocr = create_ocr(provider="easyocr", languages=['en'], gpu=False)

# Extract text from image bytes
with open("image.png", "rb") as f:
    image_bytes = f.read()
text = tesseract_ocr.bytes_to_text(image_bytes)

# Extract text from PIL Image
from PIL import Image
image = Image.open("image.png").convert("RGB")
text = tesseract_ocr.image_to_text(image)
```

### Advanced Usage

```python
from ocr import OCR

# Create OCR with custom parameters
ocr = OCR(
    provider="tesseract",
    lang="eng",
    config="--psm 6"  # Page segmentation mode
)

# Or with EasyOCR
ocr = OCR(
    provider="easyocr",
    languages=['en', 'es'],  # Multiple languages
    gpu=True  # Use GPU if available
)

# Get provider information
provider_name = ocr.get_provider_name()
print(f"Using provider: {provider_name}")
```

### Integration with extract_embeddings.py

The OCR module is now integrated into the main extraction pipeline. You can specify the OCR provider and parameters via command line arguments:

```bash
# Use Tesseract (default)
python extract_embeddings.py --extract-image-text --ocr-method tesseract --ocr-lang eng

# Use EasyOCR
python extract_embeddings.py --extract-image-text --ocr-method easyocr --ocr-lang en --ocr-gpu false

# Use EasyOCR with GPU
python extract_embeddings.py --extract-image-text --ocr-method easyocr --ocr-gpu true
```

## Environment Variables

You can also configure OCR settings via environment variables:

```bash
export WF_OCR_METHOD=easyocr
export WF_OCR_LANG=en
export WF_OCR_GPU=false
python extract_embeddings.py --extract-image-text
```

## Provider-Specific Parameters

### Tesseract

- `lang`: Language code (default: "eng")
- `config`: Tesseract configuration string (default: "")

### EasyOCR

- `languages`: List of language codes (default: ['en'])
- `gpu`: Whether to use GPU (default: False)

## Testing

Run the test script to verify OCR functionality:

```bash
python test_ocr.py path/to/test/image.png
```

This will test both Tesseract and EasyOCR providers with the provided image.

## Error Handling

The module includes comprehensive error handling:

- **Import Errors**: Clear error messages when required dependencies are missing
- **OCR Errors**: Graceful handling of OCR processing errors with logging
- **Input Validation**: Type checking for input parameters

## Logging

The module uses Python's logging system. Configure logging level as needed:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

## Migration from Existing Code

The new OCR module is backward compatible. Existing code using direct `pytesseract` calls can be gradually migrated:

### Before (Direct pytesseract usage)
```python
import pytesseract
from PIL import Image

image = Image.open("image.png")
text = pytesseract.image_to_string(image)
```

### After (Using OCR module)
```python
from ocr import create_ocr

ocr = create_ocr(provider="tesseract")
image = Image.open("image.png")
text = ocr.image_to_text(image)
```

## Performance Considerations

- **Tesseract**: Generally faster, good for simple text extraction
- **EasyOCR**: More accurate for complex layouts, supports multiple languages, can use GPU acceleration
- **GPU Usage**: EasyOCR with GPU can be significantly faster for batch processing

## Troubleshooting

### Common Issues

1. **Tesseract not found**: Install tesseract-ocr system package
2. **EasyOCR import error**: Install easyocr with `pip install easyocr`
3. **GPU issues**: Ensure CUDA is properly installed for GPU support
4. **Language support**: Download required language packs for Tesseract

### Debug Mode

Enable debug logging to troubleshoot issues:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```
