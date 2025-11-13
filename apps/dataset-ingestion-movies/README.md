# Dataset Ingestion Workflow For Modified TMDB Dataset

This workflow can ingest [TMDB dataset](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata) into ApertureDB.
In addition to persisting all the information of the dataset, the ingestion depicts how to build on top of the original data by adding more features (like posters).

This Demo also associates the posters for these movies from the dataset available from the following dataset:
https://www.kaggle.com/datasets/pankajmaulekhi/tmdb-top-10000-movies-updated-till-2025
> The posters form this dataset are put in a storage bucket hosted by ApertureData.

Further, it maintains the information as a knowledge graph, which can be queried and visualised in some interesting ways to make it useful.

## Database details

dataset-ingestion-movies adds all the records from TMDB dataset to the ApertureDB instance.

#### Objects Added to the Database

```mermaid
erDiagram
    MOVIE {
        string id
        string movie_id
        string title
        int budget
        string overview
        float popularity
        string tagline
        float vote_average
        int vote_count
        string dataset_name
        label movie
    }
    PROFESSIONAL {
        string name
        int gender
        string dataset_name
    }
    KEYWORD {

    }
    MOVIE }o--o{ PROFESSIONAL : HAS_CAST
    MOVIE }o--o{ PROFESSIONAL : HAS_CREW
    MOVIE }o--o{ GENRE : HAS_GENRE
    MOVIE }o--o{ SPOKEN_LANGUAGE : HAS_SPOKEN_LANGUAGE
    MOVIE }o--o{ KEYWORD : HAS_KEYWORD
    MOVIE }o--o{ PRODUCTION_COMPANY : HAS_PRODUCTION_COMPANY
    MOVIE ||--|| TAGLINE_EMBEDDING : HAS_TAGLINE_EMBEDDING
    MOVIE |o--|| POSTER: HAS_POSTER

```



After a successful ingestion, the following types of objects are typically added to ApertureDB:

- **MOVIE**
- **PROFESSIONAL**: Crew and Cast associated with the movie
- **KEYWORD**: Each data item (e.g., row, record) is stored as an entity.
- **Image**: Posters for some of the movies.
- **SPOKEN_LANGUAGE**
- **GENRE**



## Running in docker

```
docker run \
           -e RUN_NAME=ingestion \
           -e DB_HOST=workflowstesting.gcp.cloud.aperturedata.dev \
           -e DB_PASS="password" \
           aperturedata/workflows-dataset-ingestion-movies
```

How dataset ingestion demos work:

1. **Cleanup**: Removes all objects that have a property called dataset_name, and it's value as 'tmdb_5000'.
2. **Ingestion**: It changes the flat records from the croissant url of the dbs and stores it in property graph.
3. **Completion**: Once complete, the dataset is available in the database for querying and further processing.

### Workflow Diagram


## Cleaning up

Executing the [query](https://github.com/aperture-data/workflows/blob/main/apps/ingest-croissant/app/delete_dataset_by_url.json) against the instance of ApertureDB will selectively clean the DB of the ingested Croissant dataset, if the constraint is specified in selection of the DatasetModel Entity. Here's an example:

```json
[
    {
        "DeleteEntity": {
            "constraints": {
                "dataset_name": ["==", 'tmdb_5000']
            }
        }
    },
    {
        "DeleteDescriptorSet": {
            "constraints": {
                "dataset_name": ["==", 'tmdb_5000']
            }
        }
    }
    ]
```