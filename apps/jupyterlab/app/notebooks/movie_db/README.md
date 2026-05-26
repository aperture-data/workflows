# Movie DB Notebooks

This folder contains notebooks to query a knowledge graph using ApertureDB.
To run these notebooks, you need to have the TMDB dataset ingested into your database instance.

## Prerequisites

- [Dataset Ingestion (Movies)](https://github.com/aperture-data/workflows/tree/main/apps/dataset-ingestion-movies) workflow must be executed to populate the database with the schema, cast, and movie data, including embeddings for poster images and taglines.
- A running ApertureDB instance with Python client configured.
- `OPENAI_API_KEY` environment variable configured if you intend to run the natural language queries notebook (`tmdb_queries_nl.ipynb`).

## Notebooks

- `tmdb_queries.ipynb`: Demonstrates how to perform property graph queries using the Python API.
- `tmdb_queries_nl.ipynb`: Demonstrates how to perform natural language queries on the knowledge graph using LlamaIndex integration.
- `tmdb_vector_search.ipynb`: Showcases multimodal semantic search combining vector embeddings (images, taglines) and metadata constraints.
- `tmdb_visualize.ipynb`: Shows how to visualize connections and relationships from the graph.
