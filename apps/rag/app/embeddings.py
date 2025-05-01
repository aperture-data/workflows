import torch
import numpy as np
import hashlib
import os
from typing import List, Union
from langchain_core.embeddings import Embeddings

# backend model pretrained
# Keep this short to reduce docker build time and image size
SUPPORTED_MODELS = [
    "openclip ViT-B-32 laion2b_s34b_b79k",
    "openclip ViT-L-14 laion2b_s32b_b82k",
    "openclip RN50 yfcc15m",
    "clip ViT-B/32 openai"
]

DEFAULT_MODEL = SUPPORTED_MODELS[0]

# Do not change this text
# It is used to generate a fingerprint for the model
FINGERPRINT_TEXT = "ApertureDB unifies multimodal data, knowledge graphs, and vector search into a single database solution for rapid AI deployments at enterprise scale."


class BatchEmbedder(Embeddings):
    def __init__(self, model_spec: str, device: str = None):
        self.model_spec = model_spec
        self.backend, self.model_name, self.pretrained = \
            self._parse_model_spec(model_spec)
        # This should be "cpu" during docker build
        self.device = torch.device(device or (
            "cuda" if torch.cuda.is_available() else "cpu"))
        self.model = None
        self.tokenizer = None
        self._load_model()

    def _parse_model_spec(self, spec: str):
        parts = spec.split()
        if len(parts) != 3:
            raise ValueError(
                f"Invalid model spec: '{spec}'. Expected format 'backend model pretrained'")
        return parts[0].lower(), parts[1], parts[2]

    def _load_model(self):
        if self.backend == "openclip":
            import open_clip
            self.model, _, _ = open_clip.create_model_and_transforms(
                model_name=self.model_name,
                pretrained=self.pretrained,
                device=self.device
            )
            self.model.eval()
            self.tokenizer = open_clip.get_tokenizer(self.model_name)
            self._tokenize = lambda texts: self.tokenizer(
                texts).to(self.device)

        elif self.backend == "clip":
            import clip
            model_id = self.model_name
            self.model, _ = clip.load(
                model_id, device=self.device, download_root="/root/.cache/clip")
            self.model.eval()
            self._tokenize = lambda texts: clip.tokenize(texts).to(self.device)
        else:
            raise ValueError(f"Unsupported backend: {self.backend}")

    def embed(self, inputs: Union[str, List[str]]) -> Union[np.ndarray, List[np.ndarray]]:
        if isinstance(inputs, str):
            return self._embed_batch([inputs])[0]
        elif isinstance(inputs, list):
            return self._embed_batch(inputs)
        else:
            raise TypeError("Input must be a string or a list of strings")

    def _embed_batch(self, texts: List[str]) -> List[np.ndarray]:
        with torch.no_grad():
            tokens = self._tokenize(texts)
            features = self.model.encode_text(tokens)
            features = features / features.norm(dim=-1, keepdim=True)
            return features.cpu().numpy()

    # Support the LangChain interface
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        return [self.embed(text) for text in texts]

    def embed_query(self, query: str) -> List[float]:
        return self.embed(query)

    def fingerprint(self, canonical_text: str = FINGERPRINT_TEXT) -> np.ndarray:
        return self.embed(canonical_text)

    def fingerprint_hash(self, canonical_text: str = FINGERPRINT_TEXT) -> str:
        vec = self.fingerprint(canonical_text)
        return hashlib.sha256(vec.tobytes()).hexdigest()

    def dimensions(self) -> int:
        return self.model.visual.output_dim if hasattr(self.model, 'visual') else self.model.output_dim

    def summarize(self, canonical_text: str = FINGERPRINT_TEXT):
        vec = self.fingerprint(canonical_text)
        print(f"[INFO] Model Spec: {self.model_spec}")
        print(f"[INFO] Backend: {self.backend}")
        print(f"[INFO] Model: {self.model_name}")
        print(f"[INFO] Pretrained: {self.pretrained}")
        print(f"[INFO] Device: {self.device}")
        print(f"[INFO] Embedding Dim: {vec.shape[0]}")
        print(
            f"[INFO] Fingerprint Hash: {self.fingerprint_hash(canonical_text)}")
        print(f"[INFO] First 5 dims: {np.round(vec[:5], 4).tolist()}")

    def metric(self):
        # For now we believe all models are cosine similarity
        return "CS"


# Invoke this module directly to warm the cache
if __name__ == "__main__":
    for spec in SUPPORTED_MODELS:
        print(f"\n=== Warming cache for {spec} ===")
        be = BatchEmbedder(model_spec=spec, device="cpu")
        be.summarize()
