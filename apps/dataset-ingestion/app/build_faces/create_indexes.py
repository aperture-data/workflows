from aperturedb.Utils import Utils
from aperturedb.CommonLibrary import create_connector

utils = Utils(create_connector())

utils.create_entity_index("image:Images", "id")
utils.create_entity_index("image:Images", "image_id")
utils.create_entity_index("image:Images", "adb_image_id")
utils.create_entity_index("image:Images", "adb_image_sha256")
utils.create_entity_index("image:Images", "celebahq_id")


# Images does lots of search by uniqueid


# Demo has query by labels.
utils.create_entity_index("bounding_box:BoundingBoxes", "_label")
utils.create_entity_index("polygon:Polygons", "_label")

# Demo has embeddings
utils.create_entity_index("descriptor:ViT-B/16", "_create_txn")
utils.create_entity_index("descriptor:ViT-B/16", "id")
