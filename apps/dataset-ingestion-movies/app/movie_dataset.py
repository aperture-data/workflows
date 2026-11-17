"""Load the original TMDB Croissant data from public or explicit local inputs."""
import json
from pathlib import Path

import mlcroissant as mlc


CROISSANT_URL = "https://www.kaggle.com/datasets/tmdb/tmdb-movie-metadata/croissant/download"


def load_movie_dataset(metadata_file=None, archive_file=None):
    """Preserve Croissant validation and optionally map its original ZIP locally."""
    source = CROISSANT_URL
    if metadata_file is not None:
        source = json.loads(Path(metadata_file).read_text(encoding="utf-8"))
        if not isinstance(source, dict):
            raise ValueError("Croissant metadata must be a JSON object")

    if archive_file is None:
        return mlc.Dataset(source)

    # This ID is declared by the original TMDB metadata. Croissant validates
    # the mapping and keeps the containedIn references for its two CSV files.
    return mlc.Dataset(
        source,
        mapping={"archive.zip": str(Path(archive_file).resolve(strict=True))},
    )
