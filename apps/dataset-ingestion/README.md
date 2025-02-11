Dataset Ingestion App
=====================

This workflow can ingest some public datasets into an ApertureDB instance.

ApertureData maintains derivatives of 2 popular public datasets.

## COCO dataset
#### URL: https://cocodataset.org/#home
Note: we ingest Stuff segmentation annotations from the various tasks available.

### Whats the derivative part?
In addition to images, bounding boxes, and segmentation masks, this ingestion will contain Embeddings generated using [CLIP](https://github.com/openai/CLIP), using ViT-B16.

### Contents
| Objects | Count | Notes|
| --- | --- |
| Images (val) | 5000 |
| Images (train) | 128287 |
| Descriptors (val) | 5000 |
| Descriptors (train) | 118287 |
| Polygons (val) | 36335 |
| Polygons (train) | 849916 |
| Bounding boxes (val) | 36781 |
| Bounding boxes (train) | 860001 |

## CelebA dataset
#### URL: https://mmlab.ie.cuhk.edu.hk/projects/CelebA.html
The ingestion comprises of CelebA and CelebA-hq images along with the segmentation masks on CelebA-hq images.

The CelebA also gets associated embeddings generated from 2 models.

[Facenet](https://github.com/timesler/facenet-pytorch), using MTCNN and InceptionResnetV1
[CLIP](https://github.com/openai/CLIP), using ViT-B/16

### Contents
| Objects | Count | Notes|
| --- | --- |
| Images (cropped) | 202468 |
| Descriptors(CLIP) | 202468 |
| Descriptors (facenet) | 204015 |
| Images (HQ) | 1547 |
| Polygons | 17697 |
| Bounding boxes | 17697 |
