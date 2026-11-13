"""Name the concrete classes in this workflow's generated connection CSVs."""
import csv
from pathlib import Path


MEDIA_CLASSES = {
    "_Image": "image:Images", "_Video": "video:Videos", "_Blob": "blob:Blobs",
    "_BoundingBox": "bounding_box:BoundingBoxes", "_Polygon": "polygon:Polygons",
}


def prepare_connections(directory):
    """Update workflow-owned CSV headers; derive each vector class from its metadata CSV."""
    for path in Path(directory).rglob("*.csv"):
        with path.open(newline="") as source:
            reader = csv.reader(source)
            header = next(reader, [])
        if not header or header[0] != "ConnectionClass":
            continue
        descriptor_class = None
        if any(name.startswith("_Descriptor@") for name in header):
            metadata = path.with_name(path.name.replace("connection", "metadata"))
            with metadata.open(newline="") as source:
                record = next(csv.DictReader(source), None)
            if record:
                descriptor_class = record["set"]
            else:
                # Empty connection and descriptor files have no rows to load.
                continue
        rewritten = []
        for name in header:
            category, separator, prop = name.partition("@")
            if separator:
                category = ("descriptor:" + descriptor_class if category == "_Descriptor"
                            else MEDIA_CLASSES.get(category, category))
                name = category + separator + prop
            rewritten.append(name)
        if rewritten == header:
            continue
        temporary = path.with_suffix(path.suffix + ".native")
        try:
            with path.open(newline="") as source, temporary.open("w", newline="") as output:
                reader, writer = csv.reader(source), csv.writer(output)
                next(reader)
                writer.writerow(rewritten)
                writer.writerows(reader)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    import sys
    prepare_connections(sys.argv[1])
