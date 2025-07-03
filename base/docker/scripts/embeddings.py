import torch
import numpy as np
import hashlib
from typing import List, Union, Literal, Optional
import cv2
from PIL import Image

# provider model pretrained
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


class Embedder():
    def __init__(self,
                 provider: Literal["clip", "openclip"] = None,
                 model_name: str = None,
                 pretrained: str = None,
                 device: Optional[Literal["cpu", "cuda"]] = None):
        """Initialize the Embedder with a model specification.

        Args:
            provider (str): The provider of the model, e.g., "openclip" or "clip".
            model_name (str): The name of the model, e.g., "ViT-B-32".
            pretrained (str): The pretrained corpus, e.g., "laion2b_s34b_b79k". (Optional for CLIP)
            device (str): The device to run the model on,
        """
        # CLIP does not distinguish their pretrained corpu  s
        if provider == "clip" and not pretrained:
            pretrained = "openai"
        # This should be "cpu" during docker build
        self.device = torch.device(device or (
            "cuda" if torch.cuda.is_available() else "cpu"))
        self._load_model()  # sets self.model, self.preprocess, self.tokenizer

    def _load_model(self):
        if self.provider == "openclip":
            import open_clip
            self.model, self.preprocess, _ = open_clip.create_model_and_transforms(
                model_name=self.model_name,
                pretrained=self.pretrained,
                device=self.device
            )
            self.model.eval()
            self.tokenizer = open_clip.get_tokenizer(self.model_name)

        elif self.provider == "clip":
            import clip
            model_id = self.model_name
            # CLIP assumes pretrained corpus is "openai"
            self.model, self.preprocess = clip.load(
                model_id, device=self.device, download_root="/root/.cache/clip")
            self.model.eval()
            self.tokenizer = clip.tokenize
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def _tokenize(self, texts: List[str]) -> torch.Tensor:
        """Tokenizes the input texts using the model's tokenizer."""
        if self.tokenizer is None:
            raise ValueError(
                "Tokenizer is not initialized. Call _load_model() first.")
        return self.tokenizer(texts).to(self.device)

    def to_properties(self) -> dict:
        """Returns properties that might be added to, say, a descriptor set."""
        return {
            "embeddings": f"{self.provider} {self.model_name} {self.pretrained}",
            "embeddings_provider": self.provider,
            "embeddings_model": self.model_name,
            "embeddings_pretrained": self.pretrained,
            "embeddings_fingerprint": self.fingerprint_hash()
            "embeddings_device": str(self.device)
        }

    @classmethod
    def from_string(cls, model_spec: str, device: str = None) -> "Embedder":
        """Create an instance from a model specification string."""
        parts = model_spec.split()
        if len(parts) < 2:
            raise ValueError(
                "Model specification must include provider and model name")

        provider = parts[0].lower()
        model_name = parts[1]
        pretrained = parts[2] if len(parts) > 2 else None

        return cls(provider=provider, model_name=model_name, pretrained=pretrained, device=device)

    @classmethod
    def from_properties(cls, properties: dict, device: str = None) -> "Embedder":
        """ Create an instance from properties, say from a descriptor set.

        Args:            
            properties (dict): A dictionary of properties that must include 'embeddings'. May include "embeddings_fingerprint", "_metrics", and "_dimensions" for additional diagnostics.
            device (str): The device to run the model on, e.g., "cpu" or "cuda". If not specified, defaults to "cuda" if available, otherwise "cpu".
        """

        spec = properties.get("embeddings")
        if not spec:
            raise ValueError(
                "Properties must include 'embeddings'")

        result = cls.from_string(model_spec=spec, device=device)

        # Verify the fingerprint matches the properties
        if "embeddings_fingerprint" in properties:
            fingerprint = result.fingerprint_hash()
            if fingerprint != properties.get("embeddings_fingerprint"):
                # Don't raise an error because we see false positives
                logger.error(
                    f"Fingerprint mismatch: {fingerprint} != {properties.get('embeddings_fingerprint')}")

        if "_metrics" in properties:
            if self.metric not in properties["_metrics"]:
                logger.error(
                    f"Metric {self.metric} not found in {properties['_metrics']}")

        if "_dimensions" in properties:
            if result.dimensions != properties.get("_dimensions"):
                logger.error(
                    f"Dimensions mismatch: {result.dimensions} != {properties.get('_dimensions')}")

        return result

    def _normalize(tensor: torch.Tensor) -> torch.Tensor:
        """Normalize the tensor to unit length."""
        return tensor / tensor.norm(dim=-1, keepdim=True)

    def _as_numpy(tensor: torch.Tensor) -> np.ndarray:
        """Convert a PyTorch tensor to a NumPy array."""
        return tensor.float().cpu().numpy()

    def embed_text(self, input: str) -> np.ndarray:
        """Embed a single text input."""
        return self.embed_texts([input])[0]

    def embed_texts(self, texts: List[str]) -> List[np.ndarray]:
        with torch.no_grad():
            tokens = self._tokenize(texts)
            features = self.model.encode_text(tokens)
            features = self._normalize(features)
            features = self._as_numpy(features)
            return features

    def embed_image(self, b: bytes) -> np.ndarray:
        """Embed a single image input.

        Args:
            image (bytes): The image data in bytes format.

        Returns:
            np.ndarray: The embedded vector for the image.
        """
        nparr = np.frombuffer(b, np.uint8)
        image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
        image = Image.fromarray(image)
        image = self.preprocess(image).unsqueeze(0).to(self.device)

        features = self.model.encode_image(image)
        features = self._normalize(features)
        features = self._as_numpy(features)
        return features

    def embed_images(self, images: List[bytes]) -> List[np.ndarray]:
        """Embed a list of images.

        Args:
            images (List[bytes]): A list of image data in bytes format.

        Returns:
            List[np.ndarray]: A list of embedded vectors for the images.
        """
        return [self.embed_image(image) for image in images]

    def fingerprint(self, canonical_text: str = FINGERPRINT_TEXT) -> np.ndarray:
        result = self.embed_text(canonical_text)
        assert self.dimensions == result.shape[0], \
            f"Expected {self.dimensions} dimensions, got {result.shape[0]}"
        return result

    def fingerprint_hash(self, canonical_text: str = FINGERPRINT_TEXT) -> str:
        vec = self.fingerprint(canonical_text)
        return hashlib.sha256(vec.tobytes()).hexdigest()

    @property
    def dimensions(self) -> int:
        return self.model.visual.output_dim if hasattr(self.model, 'visual') else self.model.output_dim

    def summarize(self, canonical_text: str = FINGERPRINT_TEXT):
        vec = self.fingerprint(canonical_text)
        print(f"[INFO] {rovider: {self.provider}")
        print(f"[INFO] Model: {self.model_name}")
        print(f"[INFO] Pretrained: {self.pretrained}")
        print(f"[INFO] Device: {self.device}")
        print(f"[INFO] Embedding Dim: {self.dimensions}")
        print(
            f"[INFO] Fingerprint Hash: {self.fingerprint_hash(canonical_text)}")
        print(f"[INFO] First 5 dims: {np.round(vec[:5], 4).tolist()}")

    @property
    def metric(self):
        # For now we believe all OpenCLIP and CLIP models are cosine similarity
        return "CS"


# Invoke this module directly to warm the cache
if __name__ == "__main__":
    for spec in SUPPORTED_MODELS:
        print(f"\n=== Warming cache for {spec} ===")
        be = Embedder(model_spec=spec, device="cpu")
        be.summarize()
