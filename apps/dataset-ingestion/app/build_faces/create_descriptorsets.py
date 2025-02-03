from aperturedb.Utils import Utils
from aperturedb.CommonLibrary import create_connector

utils = Utils(create_connector())
set_names = {
    "ViT-B/16": 512,
    "facenet_pytorch_embeddings": 512
}
for set_name, dim in set_names.items():
    utils.add_descriptorset(set_name, dim,
            metric=["L2"],
            engine=["FaissFlat"])
