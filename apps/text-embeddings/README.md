# Text Extraction

This workflow take a set of raw documents (say the results of a web crawl),
extracts the text from them,
segments the text (c.f. paragraphs),
also extracts references to images,
and stores the results in an ApertureDB database.

## Database details

In addition to the single `SegmentationJob`, three main types of object are emitted:
* `Segment`: A text segment ready for embedding or similar use
* `ImageText`: The URL for an image together with context text
* `FullText`: The full extracted text for a document, with associated `Blob`

```mermaid
erDiagram 
    CrawlDocument {}
    Crawl {}
    SegmentationJob {
      datetime start_time
      string crawl_id
      string id
      datetime end_time
      number duration
      number n_segments
      number n_images
      number n_full_texts
    }
    Segment {
      string text
      string kind
      string url
    }
    ImageText {
      string image_url
      string caption
      string alt_text
      string text
      string anchor
      string text_url
    }
    FullText {
      string url
    }
    Blob {}
    Crawl {}
    Image {}
    Crawl ||--o{ CrawlDocument : crawlHasDocument
    Crawl ||--o{ SegmentationJob : crawlHasSegmentation
    CrawlDocument ||--o{ Segment : documentHasSegment
    SegmentationJob ||--o{ Segment : segmentationJobHasSegment
    CrawlDocument ||--o{ ImageText : documentHasImageText
    SegmentationJob ||--o{ ImageText : segmentationJobHasInageText
    Image }o--o| ImageText : imageHasImageText
    CrawlDocument ||--o{ FullText : documentHasFullText
    FullText ||--|| Blob : fullTextHasBlob
    SegmentationJob ||--o{ FullText : segmentationJobHasFullText
```

```mermaid
sequenceDiagram
    participant W as Text Extraction
    participant A as ApertureDB instance
    W->>A: FindEntity (Crawl)<br/>AddEntity (SegmentationJob)<br/>CreateIndex (SegmentationJob.id)
    W->>A: FindEntity (Crawl)<br/>FindEntity (CrawlDocument)
    loop For each batch
        W->>A: FindEntity (CrawlDocument)<br/>FindBlob
        A->>W: crawl documents, blobs
    end
    loop For each webpage
        W->>A: FindEntity (SegmentationJob)<br/>FindEntity (CrawlDocument)<br/>AddEntity (Segment) / AddConnection ...<br/>AddEntity (ImageText) / AddConnection / FindImage / AddConnection...<br/>AddEntity (FullText) / AddBlob / AddConnection ...
    end
    W->>A: FindEntity (SegmentationJob)<br/>UpdateEntity (SegmentationJob)
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

