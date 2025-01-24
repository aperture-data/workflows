from aperturedb.Utils import Utils, create_connector

utils = Utils(create_connector())

utils.create_entity_index("_Image", "id")
utils.create_entity_index("_Image", "image_id")
utils.create_entity_index("_Image", "adb_image_id")
utils.create_entity_index("_Image", "adb_image_sha256")
utils.create_entity_index("_Image", "celebahq_id")


#Images does lots of search by uniqueid


# Demo has query by labels.
utils.create_entity_index("_BoundingBox", "_label")
utils.create_entity_index("_Polygon", "_label")

# Demo has embeddings
utils.create_entity_index("_Descriptor", "_create_txn")
utils.create_entity_index("_Descriptor", "id")
utils.create_entity_index("_DescriptorSet", "_name")
