# Dataset Ingestion App

This workflow can ingest some public datasets into an ApertureDB instance.

ApertureData maintains adapted versions of two popular public datasets.

## COCO dataset
#### URL: https://cocodataset.org/#home
Note: we ingest Stuff segmentation annotations from the various tasks available.

### Whats the derivative part?
In addition to images, bounding boxes, and segmentation masks, this ingestion will contain Embeddings generated using [CLIP](https://github.com/openai/CLIP), using ViT-B16.

### Contents
| Objects | Count | Notes|
| --- | --- | --- |
| Images (val) | 5000 |
| Images (train) | 128287 |
| Descriptors (val) | 5000 |
| Descriptors (train) | 118287 |
| Polygons (val) | 36335 |
| Polygons (train) | 849916 |
| Bounding boxes (val) | 36781 |
| Bounding boxes (train) | 860001 |

- There will be a DescriptorSet called ViT-B/16 which will contain the embeddings generated using CLIP model.
- There will also be indexes on some properties to improve query performance.
- The details of the indexes are as follows.

| Object | Property |
| --- | --- |
| Image | id |
| Image | yfcc_id |
| Image | seg_id |
| BoundingBox | bbox_id |
| Polygon | ann_id |
| Descriptor | yfcc_id |


## CelebA dataset
#### URL: https://mmlab.ie.cuhk.edu.hk/projects/CelebA.html
The ingestion comprises of CelebA and CelebA-hq images along with the segmentation masks on CelebA-hq images.

The CelebA also gets associated embeddings generated from 2 models.

[Facenet](https://github.com/timesler/facenet-pytorch), using MTCNN and InceptionResnetV1
[CLIP](https://github.com/openai/CLIP), using ViT-B/16

### Contents
| Objects | Count | Notes|
| --- | --- | --- |
| Images (cropped) | 202468 |
| Descriptors(CLIP) | 202468 |
| Descriptors (facenet) | 204015 |
| Images (HQ) | 1547 |
| Polygons | 17697 |
| Bounding boxes | 17697 |

## Running in Docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS="password" \
           -e DATASET="coco" \
           aperturedata/workflows-dataset_ingestion
```

Parameters: 
* **`BATCH_SIZE`**: Number of objects to process in a single query. Defaults to `100`.
* **`NUM_WORKERS`**: Number of workers to execute in parallel. Defaults to `8`.
* **`CLEAN`**: Whether to delete existing data first. Defaults to `false`.
* **`SAMPLE_COUNT`**: Number of samples to ingest. Defaults to `-1`, which means all.
* **`DATASET`**: Which dataset to restart. Defaults to `coco`. Must be `coco` or `faces`.
* **`INCLUDE_TRAIN`**: Whether to include training data in addition to the validation data. Defaults to `false`. This makes ingestion much faster as it only loads a small fraction of the data, and also allows it to fit on smaller instances.
* **`LOAD_CELEBAHQ`**: Whether to include the CelebA-HQ data
* **`WF_DATA_SOURCE_AWS_BUCKET`**: AWS bucket identifier for the source data
* **`WF_DATA_SOURCE_AWS_CREDENTIALS`**: AWS credentials

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Cleaning up

TODO