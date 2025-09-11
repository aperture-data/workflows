import math
import logging


from aperturedb import QueryGenerator

logger = logging.getLogger(__name__)


class FindVideoQueryGenerator(QueryGenerator.QueryGenerator):

    """
        Generates n Video Queries for video processing
    """

    def __init__(self, pool, embedder, done_property: str, sample_rate_fps: int = 1):

        self.pool = pool
        self.embedder = embedder
        self.done_property = done_property
        self.sample_rate_fps = sample_rate_fps

        query = [{
            "FindVideo": {
                "constraints": {
                    self.done_property: ["!=", True]
                },
                "results": {
                    "count": True
                }
            }
        }]

        _, response, _ = self.pool.execute_query(query)

        try:
            total_videos = response[0]["FindVideo"]["count"]
        except:
            logger.error(
                "Error retrieving the number of videos. No videos in the db?")
            exit(0)

        if total_videos == 0:
            logger.warning("No videos to be processed. Continuing.")

        logger.info(f"Total videos to process: {total_videos}")

        self.batch_size = 2
        self.total_batches = int(math.ceil(total_videos / self.batch_size))

        self.len = self.total_batches

    def __len__(self):
        return self.len

    def getitem(self, idx):

        if idx < 0 or self.len <= idx:
            return None

        query = [
            {
                "FindVideo": {
                    "constraints": {
                        self.done_property: ["!=", True]
                    },
                    "batch": {
                        "batch_size": self.batch_size,
                        "batch_id": idx
                    },
                    "results": {
                        "list": ["_uniqueid", "_frame_count", "_fps"]
                    }
                }
            }
        ]

        return query, []


    def frames_to_embeddings(self, frames: list) -> list:
        """
        Convert frames to embeddings using the embedder.

        Args:
            frames: List of bytes array per frame

        Returns:
            List of embedding bytes
        """
        if not frames:
            return []

        try:
            # Generate embeddings
            embeddings = self.embedder.embed_images(frames)

            # Convert to bytes
            embedding_bytes = [embedding.tobytes() for embedding in embeddings]

            logger.info(f"Generated {len(embedding_bytes)} embeddings from {len(frames)} frames")
            return embedding_bytes

        except Exception as e:
            logger.exception(f"Error generating embeddings from frames: {e}")
            return []

    def response_handler(self, query, blobs, response, r_blobs):

        try:
            entities = response[0]["FindVideo"]["entities"]
        except:
            logger.exception(f"error: {response}")
            return 0


        for i, entity in enumerate(entities):
            uniqueid = entity["_uniqueid"]
            frame_count = entity["_frame_count"]
            fps = entity["_fps"]
            frames_per_sample = math.ceil(fps/self.sample_rate_fps)
            frame_spec = list(range(0, frame_count, frames_per_sample))
            query=[{
                "FindVideo": {
                    "_ref": i + 1,
                    "constraints": {
                        "_uniqueid": ["==", uniqueid]
                    },
                }
            }]
            query.append({
                "ExtractFrames": {
                    "video_ref": i + 1,
                    "frame_spec": frame_spec,
                    "as_format": "jpg",
                    "operations": [
                        {
                            "type": "resize",
                            "width": 224,
                            "height": 224
                        }
                    ]
                }
            })
            with self.pool.get_connection() as client:
                from aperturedb.CommonLibrary import execute_query
                status, response, blobs = execute_query(client, query)
                embeddings = self.frames_to_embeddings(blobs)

                query2 = [
                    {
                        "FindVideo": {
                            "_ref": 1,
                            "constraints": {
                                "_uniqueid": ["==", uniqueid]
                            },
                        }
                    }
                ]
                for i, b in enumerate(embeddings):
                    query2.append({
                        "AddClip": {
                            "video_ref": 1,
                            "_ref": i + 2,
                            "frame_number_range": {
                                "start": i * frames_per_sample,
                                "stop": min((i + 1) * frames_per_sample, frame_count)
                            }
                        }
                    })
                    query2.append({
                        "AddDescriptor": {
                            "set": self.embedder.descriptor_set,
                            "connect": {
                                "ref": i + 2,
                                "class": "ClipHasDescriptor",
                            },
                        }
                    })
                    query2.append({
                        "AddConnection": {
                            "class": "VideoHasDescriptor",
                            "src": 1,
                            "dst": i + 2,
                            "properties": {
                                "start": i * frames_per_sample,
                                "stop": min((i + 1) * frames_per_sample, frame_count)
                            }
                        }
                    })
                query2.append({
                    "UpdateVideo": {
                        "ref": 1,
                        "properties": {
                            self.done_property: True,
                        },
                    }
                    })

                status, r, _ = self.pool.execute_query(query2, embeddings)
                assert status == 0, f"Query failed: {r=}"

                logger.info(f"Total frames processed: {len(blobs)}")
