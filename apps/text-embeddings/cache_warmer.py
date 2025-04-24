#!/bin/env python3

import open_clip
import torch

OPEN_CLIP_MODELS = [
    # (model-architecture, training-copus)
    ("ViT-B-32", "laion2b_s34b_b79k"),  # 512
    ("ViT-L-14", "laion2b_s32b_b82k"),  # 768
    ("RN50", "yfcc15m"),  # 1024
]

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

for model_name, pretrained in OPEN_CLIP_MODELS:
    print(f"⏳ Preloading {model_name}/{pretrained} on {DEVICE}")
    model, _, _ = open_clip.create_model_and_transforms(
        model_name=model_name,
        pretrained=pretrained,
        device=DEVICE
    )
    print(f"Cached {model_name}/{pretrained}")
