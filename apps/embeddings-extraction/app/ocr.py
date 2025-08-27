import logging
from abc import ABC, abstractmethod
from typing import Union, Optional
from PIL import Image
from io import BytesIO

logger = logging.getLogger(__name__)


class OCR(ABC):
    """Abstract base class for OCR providers."""

    @property
    def method(self) -> str:
        """Get the name of the OCR provider."""
        return self.method
    
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
            raise ValueError(f"Unsupported OCR provider: {provider}. Supported providers: tesseract, easyocr")



class TesseractOCR(OCR):
    """Tesseract OCR provider implementation."""
    method = "tesseract"
    
    def __init__(self):
        try:
            import pytesseract
            self.pytesseract = pytesseract
        except ImportError:
            raise ImportError("pytesseract is required for TesseractOCRProvider. Install with: pip install pytesseract")
        
        
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
            logger.exception(f"Error processing image bytes with Tesseract: {e}")
            return None


class EasyOCR(OCR):
    """EasyOCR provider implementation."""
    method = "easyocr"
    
    def __init__(self):
        try:
            import easyocr
            self.easyocr = easyocr
        except ImportError:
            raise ImportError("easyocr is required for EasyOCRProvider. Install with: pip install easyocr")
        
        # Initialize EasyOCR reader
        self.languages = ['en']
        self.gpu = False
        self.reader = self.easyocr.Reader(self.languages, gpu=self.gpu)
        
    def image_to_text(self, image: Image.Image) -> Optional[str]:
        """Extract text from a PIL Image using EasyOCR."""
        try:
            # Convert PIL Image to numpy array for EasyOCR
            import numpy as np
            image_array = np.array(image)
            
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
