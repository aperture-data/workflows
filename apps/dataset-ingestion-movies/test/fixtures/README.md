# TMDB input provenance

The maintained integration assertions describe version 2 of the
[TMDB 5000 Movie Dataset](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/versions/2).
Its original public Croissant metadata is available at
[the dataset's Croissant endpoint](https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/croissant/download).
The metadata declares the original archive URL and two CSV members through
`containedIn` references.

A qualification download on 2026-11-17 verified these exact inputs:

| Input | Bytes | SHA-256 |
| --- | ---: | --- |
| Original Croissant JSON-LD | 13,596 | `bae45676ddd40273462523ac805bd0219153dd845aeaea2de65279f8b6529ac6` |
| Original ZIP archive | 9,317,430 | `0342a9748128f44e6a58e96af19d918478af73a8976b142aa4e7e0aa3c50ef1b` |

The archive's MD5, encoded as base64, is
`HtHt4F6lelD426elCLnLVg==` and matches the checksum declared in the original
metadata. It contains `tmdb_5000_credits.csv` and `tmdb_5000_movies.csv`,
each with 4,803 data rows. Their order and identifiers reproduce the existing
full-data and first-ten-movies entity and connection assertions.

## License and attribution

The original metadata credits **The Movie Database (TMDb)** as creator and
**Kaggle** as publisher. It records the license as
**Other (specified in description)**. Its description links to
[TMDB's terms](https://www.themoviedb.org/documentation/api/terms-of-use).
The [official TMDB FAQ](https://developer.themoviedb.org/docs/faq) explains its
attribution and commercial-licensing requirements.

These files are not relicensed under this repository's code license. No
permissive redistribution grant was established by the qualification download.
The original data files are not committed to this repository or included in the
product image. Preserve their source metadata, attribution and checksums when
supplying authorized local copies.

## Local qualification

The app test script fetches the original metadata and its declared archive with
host curl, verifies the SHA-256 values above, and mounts its gitignored cache
read-only in both the full and sample Compose application services. A changed
or unavailable upstream input fails the gate; it is not replaced with generated
data or accepted by retrying until validation passes. Set `MOVIES_FIXTURE_DIR`
to use another local cache directory.

Outside that test harness, supply authorized copies read-only and set `CROISSANT_METADATA_FILE` and
`CROISSANT_ARCHIVE_FILE` to their container paths. Croissant accepts the parsed
metadata directly and maps `archive.zip` to the original local file, so it
still performs the real CSV parsing and joins. Keep the existing full-data and
sample assertions, poster retrieval, and embedding generation enabled. A
metadata/count check alone does not qualify the application.
