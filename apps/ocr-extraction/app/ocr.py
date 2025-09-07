import logging
from abc import ABC, abstractmethod
from typing import Union, Optional
from PIL import Image
from io import BytesIO
import numpy as np

logger = logging.getLogger(__name__)


class OCR(ABC):
    """Abstract base class for OCR providers."""

    @property
        return self.__class__.method

    @abstractmethod
    def image_to_text(self, image: Image.Image) -> str:
        """Extract text from a PIL Image."""
        pass

    @abstractmethod
    def bytes_to_text(self, image_bytes: bytes) -> str:
        """Extract text from image bytes."""
        pass

    @classmethod
    def create(cls, provider: str = "tesseract"):
        logger.info(f"Creating OCR instance for {provider}")
        if provider == "tesseract":
            return TesseractOCR()
        elif provider == "easyocr":
            return EasyOCR()
        else:
            raise ValueError(
                f"Unsupported OCR provider: {provider}. Supported providers: tesseract, easyocr")


class TesseractOCR(OCR):
    """Tesseract OCR provider implementation."""
    method = "tesseract"

    def __init__(self):
        try:
            import pytesseract
            self.pytesseract = pytesseract
        except ImportError:
            raise ImportError(
                "pytesseract is required for TesseractOCRProvider. Install with: pip install pytesseract")

    def image_to_text(self, image: Image.Image) -> Optional[str]:
        """Extract text from a PIL Image using Tesseract."""
        try:
            text = self.pytesseract.image_to_string(image)
            return text.strip()
        except Exception as e:
            logger.exception(f"Error extracting text with Tesseract: {e}")
            return None

    def bytes_to_text(self, image_bytes: bytes) -> Optional[str]:
        """Extract text from image bytes using Tesseract."""
        try:
            image = Image.open(BytesIO(image_bytes)).convert("RGB")
            return self.image_to_text(image)
        except Exception as e:
            logger.exception(
                f"Error processing image bytes with Tesseract: {e}")
            return None


class EasyOCR(OCR):
    """EasyOCR provider implementation.

    Note that EasyOCR is very memory greedy, so its use may be the source of OOM (137) errors.
    """
    method = "easyocr"
    max_image_size = 1024
    quantize = True
    languages = ['en']
    gpu = False

    def __init__(self):
        try:
            import easyocr
            self.easyocr = easyocr
        except ImportError:
            raise ImportError(
                "easyocr is required for EasyOCRProvider. Install with: pip install easyocr")

        # Log memory usage before initialization
        try:
            import psutil
            process = psutil.Process()
            memory_before = process.memory_info().rss / 1024 / 1024  # MB
            logger.info(
                f"Memory usage before EasyOCR initialization: {memory_before:.2f} MB")
        except ImportError:
            logger.warning("psutil not available for memory monitoring")

        # Initialize EasyOCR reader with memory-efficient settings

        # Add model download caching and memory optimization
        logger.info("Initializing EasyOCR reader...")
        self.reader = self.easyocr.Reader(
            self.languages,
            gpu=self.gpu,
            # Use quantized models for ~4x lower memory usage
            quantize=self.quantize,
        )

        # Log memory usage after initialization
        try:
            import psutil
            process = psutil.Process()
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_increase = memory_after - memory_before
            logger.info(
                f"Memory usage after EasyOCR initialization: {memory_after:.2f} MB (increase: {memory_increase:.2f} MB)")
        except ImportError:
            pass

    def image_to_text(self, image: Image.Image) -> Optional[str]:
        """Extract text from a PIL Image using EasyOCR."""
        try:
            # Resize image to 1024x1024 if it's larger; retain aspect ratio
            if image.width > self.max_image_size or image.height > self.max_image_size:
                aspect_ratio = image.width / image.height
                if image.width > image.height:
                    new_width = self.max_image_size
                    new_height = int(new_width / aspect_ratio)
                else:
                    new_height = self.max_image_size
                    new_width = int(new_height * aspect_ratio)
                image = image.resize((new_width, new_height))

            # Convert PIL Image to numpy array for EasyOCR
            image_array = np.array(image)
            logger.debug(f"Image array shape: {image_array.shape}")

            # Perform OCR
            results = self.reader.readtext(image_array)

            # Extract text from results
            text_parts = []
            for (bbox, text, confidence) in results:
                if text.strip():
                    text_parts.append(text.strip())

            return " ".join(text_parts)
        except Exception as e:
            logger.exception(f"Error extracting text with EasyOCR: {e}")
            return None

    def bytes_to_text(self, image_bytes: bytes) -> Optional[str]:
        """Extract text from image bytes using EasyOCR."""
        try:
            image = Image.open(BytesIO(image_bytes)).convert("RGB")
            return self.image_to_text(image)
        except Exception as e:
            logger.exception(f"Error processing image bytes with EasyOCR: {e}")
            return None
