from shared import connection_pool, logger
from decorators import declare_mcp_resource
from typing import Annotated, Literal
from pydantic import Field
from mcp.types import ImageContent
import base64

mime_types = {
    "jpg": "image/jpeg",
    "png": "image/png"
}


@declare_mcp_resource(uri="/image/{format}/{image_id}")
def image(
    format: Annotated[Literal["jpg", "png"], Field(description="The format of the image (e.g., 'jpg', 'png')")],
    image_id: Annotated[str, Field(description="The unique identifier for the image", min_length=1)],
) -> bytes:
    """Fetch an image by its ID."""
    # TODO: Implement operations
    logger.info(f"Fetching image with ID {image_id} in format {format}")
    format = format.lower()
    assert format in [
        "jpg", "png"], f"Unsupported image format {format}. Supported formats are: jpg, png."
    query = [
        {
            "FindImage": {
                "blobs": True,
                "constraints": {
                    "_uniqueid": ["==", image_id],
                },
                "unique": True,
                "as_format": format,
            }
        }
    ]

    try:
        status, response, blobs = connection_pool.execute_query(query)
        assert status == 0, f"Error executing query: {response}"
        if not response or not blobs:
            logger.error(f"Image with ID {image_id} not found.")
            raise ValueError(f"Image with ID {image_id} not found.")
    except Exception as e:
        logger.exception(f"Failed to fetch image with ID {image_id}: {e}")
        raise ValueError(f"Failed to fetch image with ID {image_id}: {str(e)}")

    base64_data = base64.b64encode(blobs[0]).decode("utf-8")

    return ImageContent(
        type="image",
        data=base64_data,
        mimeType=mime_types.get(format, "application/octet-stream"),
    )
