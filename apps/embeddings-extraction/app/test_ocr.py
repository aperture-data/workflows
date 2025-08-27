#!/usr/bin/env python3
"""
Test script for the OCR module demonstrating usage with different providers.
"""

import sys
import os
from PIL import Image
from io import BytesIO

# Add the current directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ocr import create_ocr, OCR


def test_ocr_with_image_file(image_path: str):
    """Test OCR with an image file."""
    print(f"Testing OCR with image: {image_path}")
    
    # Read the image file
    with open(image_path, 'rb') as f:
        image_bytes = f.read()
    
    # Test with Tesseract
    print("\n--- Testing Tesseract ---")
    try:
        tesseract_ocr = create_ocr(provider="tesseract", lang="eng")
        text = tesseract_ocr.bytes_to_text(image_bytes)
        print(f"Tesseract result: {text[:200]}...")
    except Exception as e:
        print(f"Tesseract error: {e}")
    
    # Test with EasyOCR
    print("\n--- Testing EasyOCR ---")
    try:
        easyocr_ocr = create_ocr(provider="easyocr", languages=['en'], gpu=False)
        text = easyocr_ocr.bytes_to_text(image_bytes)
        print(f"EasyOCR result: {text[:200]}...")
    except Exception as e:
        print(f"EasyOCR error: {e}")


def test_ocr_with_pil_image(image_path: str):
    """Test OCR with a PIL Image object."""
    print(f"\nTesting OCR with PIL Image: {image_path}")
    
    # Load image with PIL
    image = Image.open(image_path).convert("RGB")
    
    # Test with Tesseract
    print("\n--- Testing Tesseract with PIL Image ---")
    try:
        tesseract_ocr = create_ocr(provider="tesseract", lang="eng")
        text = tesseract_ocr.image_to_text(image)
        print(f"Tesseract result: {text[:200]}...")
    except Exception as e:
        print(f"Tesseract error: {e}")
    
    # Test with EasyOCR
    print("\n--- Testing EasyOCR with PIL Image ---")
    try:
        easyocr_ocr = create_ocr(provider="easyocr", languages=['en'], gpu=False)
        text = easyocr_ocr.image_to_text(image)
        print(f"EasyOCR result: {text[:200]}...")
    except Exception as e:
        print(f"EasyOCR error: {e}")


def test_ocr_provider_info():
    """Test getting provider information."""
    print("\n--- Testing OCR Provider Info ---")
    
    tesseract_ocr = create_ocr(provider="tesseract")
    print(f"Tesseract provider name: {tesseract_ocr.get_provider_name()}")
    
    easyocr_ocr = create_ocr(provider="easyocr")
    print(f"EasyOCR provider name: {easyocr_ocr.get_provider_name()}")


def main():
    """Main test function."""
    print("OCR Module Test")
    print("=" * 50)
    
    # Test provider info
    test_ocr_provider_info()
    
    # Check if an image file was provided as argument
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
        if os.path.exists(image_path):
            test_ocr_with_image_file(image_path)
            test_ocr_with_pil_image(image_path)
        else:
            print(f"Image file not found: {image_path}")
    else:
        print("\nNo image file provided. Usage: python test_ocr.py <image_path>")
        print("Example: python test_ocr.py test_image.png")


if __name__ == "__main__":
    main()
