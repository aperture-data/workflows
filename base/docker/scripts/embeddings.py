import torch
import numpy as np
import hashlib
from typing import List, Union, Literal, Optional
import cv2
from PIL import Image
import logging
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector

# Set up logging
logger = logging.getLogger(__name__)

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


class DescriptorSetNotFoundError(Exception):
    """Exception raised when a descriptor set is not found."""

    def __init__(self, descriptor_set: str):
        super().__init__(f"Descriptor set '{descriptor_set}' not found.")
        self.descriptor_set = descriptor_set


class Embedder():
    """
    This class is a general helper for embedding text and images using OpenCLIP or CLIP models.

    The recommended way to construct an Embedder is to use one of the following methods:
    * from_descriptor_set: This will find an existing descriptor set in the database and create an Embedder instance from it. Only the descriptor set name is required. This method is recomended for read-only use cases where you want to embed query texts or images to search for with an existing descriptor set.
    * find_or_create_descriptor_set: This will find an existing descriptor set in the database or create a new one if it does not exist. You must provide the provider, model name, pretrained corpus, and descriptor set name. This method is recommended for write use cases where you want to create a new descriptor set with embeddings for texts or images, but you also want to support extention of an existing descriptor set if it already exists.

    Once you have an Embedder instance, you can use it to embed texts and images:
    * embed_text: Embed a single text input.
    * embed_texts: Embed a list of text inputs.
    * embed_image: Embed a single image input (in bytes format).
    * embed_images: Embed a list of images (in bytes format).

    All of these methods return a numpy array or a list of numpy arrays representing the embedded vectors. Call `tobytes`() on the numpy array to get the raw bytes representation of the vector, which can be used as a blob in an ApertureDB query.
    """

    def __init__(self,
                 provider: Literal["clip", "openclip"] = None,
                 model_name: str = None,
                 pretrained: str = None,
                 descriptor_set: str = None,
                 device: Optional[Literal["cpu", "cuda"]] = None):
        """Initialize the Embedder with a model specification.

        Args:
            provider (str): The provider of the model, e.g., "openclip" or "clip".
            model_name (str): The name of the model, e.g., "ViT-B-32".
            pretrained (str): The pretrained corpus, e.g., "laion2b_s34b_b79k". (Optional for CLIP)
            descriptor_set (str): The name of the descriptor set to use for this embedder.
            device (str): The device to run the model on. Default is to auto-detect.
        """
        assert provider in ["clip", "openclip"], \
            f"Unsupported provider: {provider}. Supported providers are 'clip' and 'openclip'."
        self.provider = provider

        assert model_name, "Model name must be specified."
        self.model_name = model_name

        if not pretrained and provider == "clip":
            pretrained = "openai"
        assert pretrained, "Pretrained corpus must be specified for OpenCLIP."
        self.pretrained = pretrained

        self.descriptor_set = descriptor_set

        # This should be "cpu" during docker build
        self.device = torch.device(device or (
            "cuda" if torch.cuda.is_available() else "cpu"))

        self._load_model()  # sets self.preprocess, self.tokenizer

    @staticmethod
    def parse_string(provider_model_pretrained: str) -> dict:
        """Parse a string specification into Embedder parameters.

        Args:
            * provider_model_pretrained: A string containing the provider, model name, and optionally the pretrained corpus, e.g., "openclip ViT-B-32 laion2b_s34b_b79k". This is offered as a convenience to avoid specifying each parameter separately, for example when configuring the model from an environment variable.

        Returns:
            kwargs: A dictionary containing the provider, model name, and pretrained corpus, suitable for dict interpolation into the Embedder constructor.
        """
        parts = provider_model_pretrained.split()
        if len(parts) < 2:
            raise ValueError(
                "Model specification must include provider and model name")
        provider = parts[0].lower()
        model_name = parts[1]
        pretrained = parts[2] if len(parts) > 2 else None
        return dict(provider=provider, model_name=model_name, pretrained=pretrained)

    def _load_model(self):
        """Load the model based on the provider, model name, and pretrained corpus."""
        # TOOD: Consider caching this so that multiple instances of Embedder
        # with the same provider, model name, and pretrained corpus can share the same model.
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
                model_id, device=self.device)
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

    def get_properties(self) -> dict:
        """Returns properties that might be added to, say, a descriptor set."""
        return {
            "embeddings": f"{self.provider} {self.model_name} {self.pretrained}",
            "embeddings_provider": self.provider,
            "embeddings_model": self.model_name,
            "embeddings_pretrained": self.pretrained,
            "embeddings_fingerprint": self.fingerprint_hash(),
        }

    @classmethod
    def find_or_create_descriptor_set(cls,
                                      client: Connector,
                                      provider: str = None,
                                      model_name: str = None,
                                      pretrained: str = None,
                                      descriptor_set: str,
                                      engine: str = "HNSW",
                                      device: Optional[Literal["cpu", "cuda"]] = None) -> "Embedder":
        """Find or create a descriptor set for the embedder.

        Args:
            client (Connector): The ApertureDB client to use.
            descriptor_set (str): The name of the descriptor set to use.
            provider (str): The provider of the model, e.g., "openclip" or "clip".
            model_name (str): The name of the model, e.g., "ViT-B-32".
            pretrained (str): The pretrained corpus, e.g., "laion2b_s34b_b79k". (Optional for CLIP)
            device (str): The device to run the model on. Default is to auto-detect.
        """

        try:
            self = cls.from_descriptor_set(
                client=client,
                descriptor_set=descriptor_set,
                device=device)
            logger.info(
                f"Found existing descriptor set {descriptor_set} for model {self.provider} {self.model_name} ({self.pretrained}) on {self.device}")

            # Verify that the existing descriptor set matches the requested parameters
            if provider != self.provider:
                raise ValueError(
                    f"Provider mismatch: {provider} != {self.provider}")
            if model_name != self.model_name:
                raise ValueError(
                    f"Model name mismatch: {model_name} != {self.model_name}")
            if pretrained != self.pretrained:
                raise ValueError(
                    f"Pretrained corpus mismatch: {pretrained} != {self.pretrained}")

            return self

        except DescriptorSetNotFoundError:
            logger.info(
                f"Descriptor set {descriptor_set} not found. Creating a new one.")
            # Create a new embedder instance
            self = cls(provider=provider,
                       model_name=model_name,
                       pretrained=pretrained,
                       descriptor_set=descriptor_set,
                       device=device)

            # Create the descriptor set in the database
            properties = self.get_properties()
            query = [{
                "AddDescriptorSet": {
                    "with_name": descriptor_set,
                    "properties": properties,
                    "metrics": [self.metric],
                    "dimensions": self.dimensions,
                    "engine": engine,
                }
            }]
            status, response, _ = execute_query(client, query)
            if status != 0:
                raise RuntimeError(
                    f"Failed to create descriptor set {descriptor_set}: {response}")

            logger.info(
                f"Created descriptor set {descriptor_set} for model {self.provider} {self.model_name} ({self.pretrained}) on {self.device}")
            return self

    @classmethod
    def get_query(cls, descriptor_set) -> List[dict]:
        """Get a query to find a descriptor set."""
        return [{
            "FindDescriptorSet": {
                "with_name": descriptor_set,
                "results": {
                    "list": ['embeddings_provider', 'embeddings_model', 'embeddings_pretrained', 'embeddings_fingerprint',]
                },
                "metrics": True,
                "dimensions": True,
            }
        }]

    @classmethod
    def from_descriptor_set(cls,
                            client: Connector,
                            descriptor_set: str,
                            device: str = None,
                            ) -> "Embedder":
        """Create an instance from a descriptor set name."""
        query = cls.get_query(descriptor_set)
        status, response, _ = execute_query(client, query)
        if status != 0:
            raise RuntimeError(
                f"Failed to execute query for descriptor set {descriptor_set}: {response}")
        if not response or not response[0].get("FindDescriptorSet"):
            raise ValueError(f"Unexpected response format for descriptor set {descriptor_set}: {response})

        if "entities" not in response[0]["FindDescriptorSet"]:
            # Specific error for missing descriptor set
            raise DescriptorSetNotFoundError(descriptor_set)

        entities = response[0]["FindDescriptorSet"]["entities"]
        if len(entities) == 0:
            raise ValueError(
                f"No entities found in descriptor set {descriptor_set}")
        if len(entities) > 1:
            logger.warning(
                f"Multiple entities found in descriptor set {descriptor_set}. Using the first one.")
        properties = entities[0]

        provider = properties.get("embeddings_provider")
        if not provider:
            raise ValueError(
                f"Descriptor set {descriptor_set} does not have 'embeddings_provider' property.")
        model = properties.get("embeddings_model")
        if not model:
            raise ValueError(
                f"Descriptor set {descriptor_set} does not have 'embeddings_model' property.")
        pretrained = properties.get("embeddings_pretrained")
        if not pretrained:
            raise ValueError(
                f"Descriptor set {descriptor_set} does not have 'embeddings_pretrained' property.")

        self = cls(provider=provider,
                   model_name=model,
                   pretrained=pretrained,
                   descriptor_set=descriptor_set,
                   device=device)

        if not self:
            raise ValueError(
                f"Failed to create Embedder from descriptor set {descriptor_set}")
        logger.info(
            f"Created Embedder from descriptor set {descriptor_set}: {self}")
        return self

    @staticmethod
    def _as_numpy(tensor: torch.Tensor) -> np.ndarray:
        """Convert a PyTorch tensor to a NumPy array."""
        return tensor.float().cpu().numpy()

    def embed_text(self, text: str) -> np.ndarray:
        """Embed a single text input."""
        return self.embed_texts([text])[0]

    def embed_texts(self, texts: List[str]) -> List[np.ndarray]:
        logger.debug(f"Embedding {len(texts)} texts on {self.device}")

        tokens = self._tokenize(texts)

        with torch.no_grad():
            features = self.model.encode_text(tokens)

        features = [self._as_numpy(f) for f in features]

        assert all(f.shape[0] == self.dimensions for f in features), \
            f"Expected all embeddings to have {self.dimensions} dimensions, got {[f.shape[0] for f in features]}"

        return features

    def embed_image(self, b: bytes) -> np.ndarray:
        """Embed a single image input.

        Args:
            image (bytes): The image data in bytes format (JPEG/PNG).

        Returns:
            np.ndarray: The embedded vector for the image.
        """
        return self.embed_images([b])[0]

    def embed_images(self, images: List[bytes]) -> List[np.ndarray]:
        """Embed a list of images.

        Args:
            images (List[bytes]): A list of image data in bytes format (JPEG/PNG).

        Returns:
            List[np.ndarray]: A list of embedded vectors for the images.
        """

        logger.debug(f"Embedding {len(images)} images on {self.device}")

        preprocessed = []

        for i, b in enumerate(images):
            try:
                nparr = np.frombuffer(b, np.uint8)
                image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
                image = Image.fromarray(image)
                tensor = self.preprocess(image)  # shape [C, H, W]
                preprocessed.append(tensor)
            except Exception as e:
                raise ValueError(
                    f"Failed to preprocess image {i}: {e}. Ensure the image is valid and in a supported format.")

        # Stack and move to device
        batch = torch.stack(preprocessed, dim=0).to(
            self.device)  # shape [B, C, H, W]

        with torch.no_grad():
            features = self.model.encode_image(batch)  # shape [B, D]

        features = [self._as_numpy(f) for f in features]  # List[np.ndarray]

        assert all(f.shape[0] == self.dimensions for f in features), \
            f"Expected all embeddings to have {self.dimensions} dimensions, got {[f.shape[0] for f in features]}"
        return features

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
        print(f"[INFO] Provider: {self.provider}")
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

    def __repr__(self):
        return f"<Embedder {self.provider} {self.model_name} ({self.pretrained}) on {self.device}>"


# Invoke this module directly to warm the cache
if __name__ == "__main__":
    for spec in SUPPORTED_MODELS:
        print(f"\n=== Warming cache for {spec} ===")
        be = Embedder.from_string(spec, device="cpu")
        be.summarize()
