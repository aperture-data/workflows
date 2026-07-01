import io
import math
import logging
import threading

from PIL import Image

from aperturedb import QueryGenerator


logger = logging.getLogger(__name__)

# Lazy-loaded globals
_processor = None
_model = None
_model_lock = threading.Lock()
_inference_lock = threading.Lock()

def get_model_and_processor():
    global _processor, _model
    with _model_lock:
        if _processor is None or _model is None:
            from transformers import AutoProcessor, BlipForConditionalGeneration
            _processor = AutoProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
            _model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
            _model.eval()
    return _processor, _model


class FindImageQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindImage Queries
    """

    def __init__(self, pool, caption_image_property: str, batch_size: int = 32):

        self.pool = pool
        self.caption_image_property = caption_image_property

        try:
            self.batch_size = int(batch_size)
        except ValueError:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size}")

        if self.batch_size <= 0:
            raise ValueError(f"batch_size must be a positive integer, got {batch_size}")

        query = [{
            "FindImage": {
                "constraints": {
                    self.caption_image_property + "_done": ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        status, response, _ = self.pool.execute_query(query)
        if status != 0:
            raise RuntimeError(f"Error executing query to find images: {response}")
            
        try:
            total_images = response[0]["FindImage"]["count"]
        except Exception as e:
            logger.error(f"Error retrieving the number of images: {e}")
            total_images = 0

        if total_images == 0:
            logger.warning("No images to be processed. Continuing!")
            self.total_batches = 0
            self.len = 0
            return

        logger.info(f"Total images to process: {total_images}")

        self.total_batches = int(math.ceil(total_images / self.batch_size))
        self.len = self.total_batches

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        query = [{
            "FindImage": {
                "batch": {
                    "batch_id": idx,
                    "batch_size": self.batch_size
                },
                "blobs": True,
                "constraints": {
                    self.caption_image_property + "_done": ["!=", True]
                },
                "operations": [
                    {
                        "type": "resize",
                        "width": 224,
                        "height": 224
                    }
                ],
                "results": {
                    "list": ["_uniqueid"]
                }
            }
        }]

        return query, []

    def response_handler(self, query, blobs, response, r_blobs):

        try:
            uniqueids = [i["_uniqueid"]
                         for i in response[0]["FindImage"]["entities"]]
        except Exception as e:
            logger.exception(f"error parsing uniqueids from response: {response}")
            raise RuntimeError(f"error parsing uniqueids from response: {response}") from e

        if len(uniqueids) != len(r_blobs):
            logger.error(f"Mismatch in response: {len(uniqueids)} uniqueids vs {len(r_blobs)} blobs")
            query_fail = []
            ref_idx = 1
            for uid in uniqueids:
                query_fail.append({
                    "FindImage": {"_ref": ref_idx, "constraints": {"_uniqueid": ["==", uid]}}
                })
                query_fail.append({
                    "UpdateImage": {
                        "ref": ref_idx,
                        "properties": {
                            self.caption_image_property + "_done": True,
                            self.caption_image_property + "_failed": True,
                            self.caption_image_property + "_error": "Mismatch in blob response"
                        }
                    }
                })
                ref_idx += 1
            status, r, _ = self.pool.execute_query(query_fail)
            if status != 0:
                logger.error(f"Failed to update images on mismatch: {r}")
            return 0

        processor, model = get_model_and_processor()
        import torch

        valid_uniqueids = []
        captions = []
        failed_uniqueids = []
        failed_reasons = []

        images_to_process = []
        texts = []
        uids_to_process = []

        for uid, b in zip(uniqueids, r_blobs):
            try:
                image = Image.open(io.BytesIO(b)).convert("RGB")
                images_to_process.append(image)
                texts.append("A picture of")
                uids_to_process.append(uid)
            except Exception as e:
                logger.error(f"Failed to load image {uid}: {e}")
                failed_uniqueids.append(uid)
                failed_reasons.append(str(e))

        if images_to_process:
            try:
                inputs = processor(images=images_to_process, text=texts, return_tensors="pt", padding=True)
                with _inference_lock:
                    with torch.no_grad():
                        outputs = model.generate(**inputs)
                batch_captions = processor.batch_decode(outputs, skip_special_tokens=True)
                for uid, caption in zip(uids_to_process, batch_captions):
                    valid_uniqueids.append(uid)
                    captions.append(caption)
            except Exception as e:
                logger.error(f"Failed to process batch, falling back to per-image: {e}")
                for uid, img, txt in zip(uids_to_process, images_to_process, texts):
                    try:
                        inputs = processor(images=img, text=txt, return_tensors="pt", padding=True)
                        with _inference_lock:
                            with torch.no_grad():
                                outputs = model.generate(**inputs)
                        caption = processor.decode(outputs[0], skip_special_tokens=True)
                        valid_uniqueids.append(uid)
                        captions.append(caption)
                    except Exception as single_e:
                        logger.error(f"Failed to process image {uid} individually: {single_e}")
                        failed_uniqueids.append(uid)
                        failed_reasons.append(str(single_e))

        if not valid_uniqueids and not failed_uniqueids:
            return 0

        query = []
        ref_idx = 1

        for uniqueid, caption in zip(valid_uniqueids, captions):
            query.append({
                "FindImage": {
                    "_ref": ref_idx,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            })

            query.append({
                "UpdateImage": {
                    "ref": ref_idx,
                    "properties": {
                        self.caption_image_property: caption,
                        self.caption_image_property + "_done": True
                    },
                }
            })
            ref_idx += 1

        for uniqueid, reason in zip(failed_uniqueids, failed_reasons):
            query.append({
                "FindImage": {
                    "_ref": ref_idx,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            })

            query.append({
                "UpdateImage": {
                    "ref": ref_idx,
                    "properties": {
                        self.caption_image_property + "_done": True,
                        self.caption_image_property + "_failed": True,
                        self.caption_image_property + "_error": reason
                    },
                }
            })
            ref_idx += 1

        status, r, _ = self.pool.execute_query(query)
        if status != 0:
            logger.error(f"Query failed: {r}")
            raise RuntimeError(f"Query failed: {r}")

        return len(valid_uniqueids)
