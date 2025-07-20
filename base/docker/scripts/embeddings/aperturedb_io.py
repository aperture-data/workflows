from typing import Optional


def get_aperturedb_io_base():
    import sys
    import os

    # Stupid trick to make sure the parent directory is in the path
    sys.path.insert(0, os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..')))
    from aperturedb_io_base import AperturedbIOBase
    return AperturedbIOBase


AperturedbIOBase = get_aperturedb_io_base()


class AperturedbIO(AperturedbIOBase):
    """
    Class for handling ApertureDB IO operations with additional functionality.
    Inherits from AperturedbIOBase.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def find_descriptor_set(self, descriptor_set_id: str) -> Optional[dict]:
        """
        Find a descriptor set by its ID.

        Args:
            descriptor_set_id (str): The ID of the descriptor set to find.

        Returns:
            properties: 
        """
        results, _ = self.execute_query(
            [{
                "FindDescriptorSet": {
                    "with_name": descriptor_set,
                    "results": {
                        "list": ['embeddings_provider', 'embeddings_model', 'embeddings_pretrained', 'embeddings_fingerprint',]
                    },
                    "metrics": True,
                    "dimensions": True,
                }
            }])

        if not response or not response[0].get("FindDescriptorSet"):
            raise ValueError(
                f"Unexpected response format for descriptor set {descriptor_set}: {response}")

        if "entities" not in response[0]["FindDescriptorSet"]:
            # Specific error for missing descriptor set
            raise DescriptorSetNotFoundError(descriptor_set)

        entities = response[0]["FindDescriptorSet"]["entities"]
        if len(entities) == 0:
            raise ValueError(
                f"No entities found in descriptor set {descriptor_set}")
        if len(entities) > 1:
            logger.warning(
                f"Multiple entities found in descriptor set {descriptor_set}. Using the first one.")

        properties = entities[0]

        return properties

    def add_descriptor_set(self,
                           descriptor_set: str,
                           metric: str,
                           dimensions: int,
                           engine: str,
                           properties: Optional[dict] = None,
                           ) -> None:
        """
        Add a new descriptor set to the database.

        Args:
            descriptor_set (dict): The descriptor set to add.
            metric (str): The metric to use for the descriptor set.
            dimensions (int): The number of dimensions for the descriptor set.
            engine (str): The engine to use for the descriptor set.
            properties (Optional[dict]): Additional properties for the descriptor set.

        Returns:
            str: The ID of the newly created descriptor set.
        """
        query = [{
            "AddDescriptorSet": {
                "descriptor_set": descriptor_set,
                "metric": metric,
                "dimensions": dimensions,
                "engine": engine,
                **({"properties": properties} if properties else {}),
            }
        }]
        self.execute_query(query)
