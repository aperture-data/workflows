from typing import List, Optional, Union, Literal
from dataclasses import dataclass
import numpy as np


@dataclass
class Segment:
    id: str
    url: str
    text: str


@dataclass
class Embedding:
    segment_id: str
    url: str
    text: str
    vector: np.ndarray
