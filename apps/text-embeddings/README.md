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
           -e WF_CRAWL=abcd1234 \
           -e "WF_CSS_SELECTOR=DIV#main-content" \
           -e WF_LOG_LEVEL=INFO \
           aperturedata/text-extraction
```

Parameters: 
* **`WF_CRAWL`**: (Required) Identifier for the crawl to work on
* **`WF_CSS_SELECTOR`**: Optional CSS selector for HTML text extraction. If specified and if present in the document, only these sections of the document will have text extracted.
* **`LOG_LEVEL`**: DEBUG, INFO, WARNING, ERROR, CRITICAL. Default WARNING.

See [Common Parameters](../../README.md#common-parameters) for common parameters.

## Cleaning up

To remove all objects created by all runs of this workflow, run the following query:

```javascript
[
  {"FindEntity": {"with_class": "SegmentationJob", "_ref": 1}},
  {"FindEntity": {"with_class": "Segment", "_ref": 2}},
  {"FindEntity": {"with_class": "ImageText", "_ref": 3}},
  {"FindEntity": {"with_class": "FullText", "_ref": 4}},
  {"FindBlob": {"is_connected_to": {"ref": 4}, "_ref": 5}},
  {"DeleteBlob": {"ref": 5}},
  {"DeleteEntity": {"ref": 4}}
  {"DeleteEntity": {"ref": 3}}
  {"DeleteEntity": {"ref": 2}}
  {"DeleteEntity": {"ref": 1}}
]
```

