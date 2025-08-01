import aiohttp
from typing import List, Iterator, Optional
import os
import logging
import openai
import json
import asyncio

logger = logging.getLogger(__name__)

# Trade-off here. Too many models will give us a big docker image.
HF_PRELOAD_MODELS = [
    "TinyLlama/TinyLlama-1.1B-Chat-v1.0",
    # "microsoft/phi-2"
    # "mistral-7b",
    # "llama-2-7b-chat",
]
DEFAULT_PROVIDER = "huggingface"

# We have a different default model for each provider.
DEFAULT_MODELS = {
    "openai": "gpt-3.5-turbo",
    "together": "mistralai/Mistral-7B-Instruct-v0.2",
    "groq": "llama3-8b-8192",
    "cohere": "command-r-plus",
    "huggingface": HF_PRELOAD_MODELS[0],
}


class LLM:
    """Standard interface so the rest of your app can call .predict(prompt)"""

    async def predict(self, prompt: str) -> str:
        assert type(self).stream_predict is not LLM.stream_predict, \
            "LLM subclasses must implement stream_predict() or predict()"
        parts = []
        async for token in self.stream_predict(prompt):
            parts.append(token)
        # Or " ".join(parts) depending on how your streamer yields
        return "".join(parts)

    async def stream_predict(self, prompt: str) -> Iterator[str]:
        assert type(self).predict is not LLM.predict, \
            "LLM subclasses must implement stream_predict() or predict()"
        yield await self.predict(prompt)

    def validate(self):
        """Check if the LLM is ready to use. """

        async def _validate():
            print(
                f"> Validating LLM {self.__class__.__name__}, provider={self.provider}, model={self.model}")
            response = await self.predict("Hello! Just testing LLM. Ignore this.")
            if not response:
                print(response)
                raise ValueError(
                    f"LLM is not ready to use. Double-check your API key and model. {response}")

        print(
            f"Validating LLM {self.__class__.__name__}, provider={self.provider}, model={self.model}")
        # validate needs to be synchronous to be called from the top-level code
        asyncio.run(_validate())


class OpenAILLM(LLM):
    def __init__(self, model: str, api_key: str):
        self.model = model
        self.client = openai.AsyncOpenAI(api_key=api_key)

    async def stream_predict(self, prompt: str):
        response = await self.client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            stream=True,
        )

        async for chunk in response:
            if chunk.choices:
                delta = chunk.choices[0].delta
                if delta and delta.content:
                    yield delta.content


class TogetherLLM(LLM):
    def __init__(self, model: str, api_key: str):
        self.model = model
        self.api_key = api_key

    async def stream_predict(self, prompt: str):
        url = "https://api.together.xyz/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                async for line in resp.content:
                    if line.startswith(b"data:"):
                        data = line[len(b"data:"):].strip()
                        if data == b"[DONE]":
                            break
                        chunk = json.loads(data)
                        delta = chunk.get("choices", [{}])[0].get(
                            "delta", {}).get("content", "")
                        if delta:
                            yield delta


class GroqLLM(LLM):
    def __init__(self, model: str, api_key: str):
        self.model = model
        self.api_key = api_key

    async def stream_predict(self, prompt: str):
        url = "https://api.groq.com/openai/v1/chat/completions"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                async for line in resp.content:
                    if line.startswith(b"data:"):
                        data = line[len(b"data:"):].strip()
                        if data == b"[DONE]":
                            break
                        chunk = json.loads(data)
                        delta = chunk.get("choices", [{}])[0].get(
                            "delta", {}).get("content", "")
                        if delta:
                            yield delta


class CohereLLM(LLM):
    def __init__(self, model: str, api_key: str):
        self.model = model
        self.api_key = api_key

    async def stream_predict(self, prompt: str):
        url = "https://api.cohere.ai/v1/chat"

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self.model,
            "message": prompt,
            "stream": True,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=payload) as resp:
                async for line in resp.content:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        chunk = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if chunk.get("event_type") == "text-generation":
                        text = chunk.get("text", "")
                        if text:
                            yield text


class HuggingFaceLLM(LLM):
    def __init__(self, model_id: str):
        """
        model_id: Hugging Face model repo ID, like 'mistralai/Mistral-7B-v0.1'
        """
        import torch
        from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

        device = 0 if torch.cuda.is_available() else -1

        tokenizer = AutoTokenizer.from_pretrained(model_id)

        model = AutoModelForCausalLM.from_pretrained(
            model_id,
            device_map="auto",
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
        )

        self.pipeline = pipeline(
            "text-generation",
            model=model,
            tokenizer=tokenizer,
            max_new_tokens=128,
            temperature=0.2,
            top_p=0.9,
            repetition_penalty=1.2,
            do_sample=False,
            return_full_text=False,
        )

    async def predict(self, prompt: str) -> str:
        output = self.pipeline(prompt)
        return output[0]['generated_text']


def load_llm(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: str = None
) -> LLM:
    """Factory function to load LLM"""

    if provider is None:
        provider = DEFAULT_PROVIDER

    if model is None:
        model = DEFAULT_MODELS[provider]

    logger.info(f"Loading LLM: provider={provider}, model={model}")

    if provider == "openai":
        if not api_key:
            raise ValueError("OPENAI API key required for OpenAI provider.")
        result = OpenAILLM(model, api_key)

    elif provider == "together":
        if not api_key:
            raise ValueError(
                "TOGETHER API key required for TogetherAI provider.")
        result = TogetherLLM(model, api_key)

    elif provider == "groq":
        if not api_key:
            raise ValueError("GROQ API key required for Groq provider.")
        result = GroqLLM(model, api_key)

    elif provider == "cohere":
        if not api_key:
            raise ValueError("COHERE API key required for Cohere provider.")
        result = CohereLLM(model, api_key)

    elif provider == "huggingface":
        result = HuggingFaceLLM(model)
    else:
        raise ValueError(f"Unsupported LLM provider: {provider}")

    result.model = model
    result.provider = provider
    # result.validate()

    return result


def main():
    """Warm up the HF model cache by loading the local models."""
    import sys

    logger.info("[Cache Warmer] Starting LLM pre-load...")

    try:
        for model in HF_PRELOAD_MODELS:
            llm = load_llm(model=model)
        logger.info("[Cache Warmer] LLM loaded successfully.")

    except Exception as e:
        logger.exception("[Cache Warmer] Failed to warm up LLM!")
        sys.exit(1)

    logger.info("[Cache Warmer] Done warming up.")


if __name__ == "__main__":
    main()
