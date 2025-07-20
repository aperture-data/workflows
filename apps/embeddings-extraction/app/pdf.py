from text_extraction.text_extractor import TextExtractor
from text_extraction.segmentation import TextSegmenter
from text_extraction.schema import TextBlock, ImageBlock, FullTextBlock

import math

import torch
import numpy as np
import clip

from aperturedb import QueryGenerator


class FindPDFQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n FindBlob Queries
    """

    def __init__(self, db, model_name):

        self.db = db

        # Choose the model to be used.
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load(model_name, device=self.device)

        query = [{
            "FindBlob": {
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "wf_embeddings_clip": ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        response, _ = db.query(query)

        try:
            total_pdfs = response[0]["FindBlob"]["count"]
        except:
            print("Error retrieving the number of PDFs. No PDFs in the db?")
            exit(0)

        if total_images == 0:
            print("No PDFs to be processed. Bye!")
            exit(0)

        print(f"Total PDFs to process: {total_pdfs}")

        self.batch_size = 32
        self.total_batches = int(math.ceil(total_pdfs / self.batch_size))

        self.len = self.total_batches

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        query = [{
            "FindBlob": {
                "blobs": True,
                "constraints": {
                    "document_type": ["==", "pdf"],
                    "wf_embeddings_clip": ["!=", True]
                },
                "batch": {
                    "batch_size": self.batch_size,
                    "batch_id": idx
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
        except:
            print(f"error: {response}")
            return 0

        desc_blobs = []

        for b in r_blobs:
            nparr = np.frombuffer(b, np.uint8)
            image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
            image = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
            image = self.preprocess(Image.fromarray(
                image)).unsqueeze(0).to(self.device)

            image_features = self.model.encode_image(image)

            if self.device == "cuda":
                image_features = image_features.float()
                desc_blobs.append(
                    image_features.detach().cpu().numpy().tobytes())
            else:
                desc_blobs.append(image_features.detach().numpy().tobytes())

        query = []
        for uniqueid, i in zip(uniqueids, range(len(uniqueids))):

            query.append({
                "FindImage": {
                    "_ref": i + 1,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            })

            query.append({
                "UpdateImage": {
                    "ref": i + 1,
                    "properties": {
                        "wf_embeddings_clip": True
                    },
                }
            })

            query.append({
                "AddDescriptor": {
                    "set": "wf_embeddings_clip",
                    "connect": {
                        "ref": i + 1
                    }
                }
            })

        # This is not nice, but we need to create a new connection
        # and this happens in parallel with many threads.
        db = self.db.create_new_connection()

        r, _ = db.query(query, desc_blobs)

        if not db.last_query_ok():
            db.print_last_response()
