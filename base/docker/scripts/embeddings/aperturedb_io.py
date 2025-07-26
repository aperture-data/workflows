from typing import Optional
from aperturedb.CommonLibrary import execute_query
from aperturedb.Connector import Connector
import logging

logger = logging.getLogger(__name__)


def find_descriptor_set(client: Connector,
                        descriptor_set: str
                        ) -> Optional[dict]:
    """
    Find a descriptor set by its ID.

    Args:
        client: The database client to execute the query.
        descriptor_set_id (str): The ID of the descriptor set to find.

    Returns:
        properties: Selected properties of the descriptor set, or None if not found.
    """
    _, response, _ = execute_query(
        client,
        [{
            "FindDescriptorSet": {
                "with_name": descriptor_set,
                "results": {
                    "list": ['embeddings_provider', 'embeddings_model', 'embeddings_pretrained', 'embeddings_fingerprint']
                },
                "metrics": True,
                "dimensions": True,
            }
        }])

    if not response or not response[0].get("FindDescriptorSet"):
        raise ValueError(
            f"Unexpected response format for descriptor set {descriptor_set}: {response}")

    if "entities" not in response[0]["FindDescriptorSet"]:
        logger.warning(f"No entities found in descriptor set {descriptor_set}")
        return None

    entities = response[0]["FindDescriptorSet"]["entities"]
    if len(entities) == 0:
        raise ValueError(
            f"No entities found in descriptor set {descriptor_set}")
    if len(entities) > 1:
        logger.warning(
            f"Multiple entities found in descriptor set {descriptor_set}. Using the first one.")

    properties = entities[0]

    logger.info(
        f"Found descriptor set {descriptor_set} with properties: {properties}")

    return properties


def add_descriptor_set(
    client: Connector,
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
    logger.info(
        f"Adding descriptor set {descriptor_set} with metric {metric}, dimensions {dimensions}, engine {engine}, properties {properties}")
    query = [{
        "AddDescriptorSet": {
            "name": descriptor_set,
            "metric": metric,
            "dimensions": dimensions,
            "engine": engine,
            **({"properties": properties} if properties else {}),
        }
    }]
    execute_query(client, query)
    if not client.last_query_ok():
        raise RuntimeError(
            f"Failed to add descriptor set {descriptor_set}: {query} {client.get_last_response_str()}")


def delete_descriptor_set(client: Connector,
                          descriptor_set: str) -> None:
    """
    Delete a descriptor set from the database.

    Args:
        client (Connector): The database client to execute the query.
        descriptor_set (str): The ID of the descriptor set to delete.
    """
    query = [{
        "DeleteDescriptorSet": {
            "with_name": descriptor_set
        }
    }]
    execute_query(client, query)
    if not client.last_query_ok():
        raise RuntimeError(
            f"Failed to add descriptor set {descriptor_set}: {query} {client.get_last_response_str()}")
    logger.info(f"Descriptor set {descriptor_set} deleted.")
