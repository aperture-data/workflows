import easyocr
import numpy as np

# Trigger download and cache population
reader = easyocr.Reader(['en'], gpu=False, quantize=True)
reader.readtext(np.zeros((100, 100, 3), dtype=np.uint8))