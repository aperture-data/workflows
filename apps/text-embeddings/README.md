# Text Extraction

This workflow take a collectiono of text segments and generates embeddings for them, storing them as descriptors.

## Database details

This workflow uses `SegmentationSpec` and `Segment`, and generates `EmbeddingsSpec`, `EmbeddingsRun`, `DescriptorSet`, and `Descriptor`.

```mermaid
erDiagram
    Segment {}
    SegmentationSpec {}
    EmbeddingsSpec {
      string segmentation_spec_id
      string id
      string model
      string mmodel_fingerprint
      number dimensions
      string metric
      string descriptorset_name
      string engine
    }
    EmbeddingsRun {}
    DescriptorSet {
      string model
      string model_fingerprint
    }
    Descriptor {
      string segment_id
      string uniqueid
      string spec_id
      string run_id
      string text
      string url
    }
    Segment {
      string text
      string kind
      string url
    }

    SegmentationSpec ||--o{ EmbeddingsSpec : segmentationSpecHasEmbeddingsSpec
    EmbeddingsSpec ||--o{ EmbeddingsRun : embeddingsSpecHasRun
    EmbeddingsSpec }o--|| DescriptorSet : embeddingsSpecHasDescriptorSet
    EmbeddingsSpec ||--o{ Descriptor : embeddingsSpecHasDescriptor
    EmbeddingsRun ||--o{ Descriptor : embeddingsRunHasDescriptor
    SegmentationSpec ||--o{ Segment : segmentationSpecHasSegment
    Segment ||--o{ Descriptor : segmentHasDescriptor
    DescriptorSet ||--o{ Descriptor: DescriptorSetToDescriptor
```

```mermaid
sequenceDiagram
    participant W as Text Embeddings
    participant A as ApertureDB instance
    W->>A: FindEntity (SegmentationSpec)<br/>AddEntity (EmbeddingsSpec)<br/>AddDescriptorSet<br/>CreateIndex (various)
    W->>A: FindEntity (SegmentationSpec)<br/>FindEntity (Segment)
    loop For each segment
        W->>A: FindEntity (EmbeddingsSpec)<br/>FindEntity (Segment)<br/>AddDescriptor <br/> AddConnection 
    end
    W->>A: FindEntity (EmbeddingsSpec)<br/>AddEntity (EmbeddingsRun)<br/>FindDescriptor<br/>AddConnection
```


## Running in docker

```
docker run \
           -e RUN_NAME=my_testing_run \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS=password \
           -e WF_LOG_LEVEL=INFO \
           -e WF_INPUT=abc123 \
           -e WF_OUTPUT=abc123 \
           aperturedata/workflows-text-embeddings
```

Parameters: 
* **`WF_LOG_LEVEL`**: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default WARNING.
* **`WF_INPUT`**: The segmentation spec identifier to use. Required unless deleting.
* **`WF_OUTPUT`**: The embeddings spec identifier to use. Defaults to UUID.
* **`WF_MODEL`**: The embedding model to use, of the form "backend model pretrained'. Default is "openclip ViT-B-32 laion2b_s34b_b79k". See [embeddings.py](app/embeddings.py)
* **`WF_ENGINE`**: The embedding engine to use, default HNSW
* **`WF_CLEAN`**: Delete existing spec before creating a new one; otherwise fail if spec exists
* **`WF_DELETE`**: Delete `WF_OUTPUT` spec; do not generate embeddings
* **`WF_DELETE_ALL`**: Delete all embedding specs; do not generate embeddings
* **`WF_DESCRIPTOR_SET`**: Descriptor set to use for embeddings; defaults to `WF_OUTPUT`


See [Common Parameters](../../README.md#common-parameters) for common parameters.

