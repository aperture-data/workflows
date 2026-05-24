from shared import connection_pool, logger
from decorators import declare_mcp_resource
from typing import Annotated, Literal
from pydantic import Field
import base64

@declare_mcp_resource(uri="/image/jpg/{image_id}", mime_type="image/jpeg")
def image_jpg(
    image_id: Annotated[str, Field(description="The unique identifier for the image", min_length=1)],
) -> bytes:
    """Fetch an image by its ID in JPG format."""
    return image(format="jpg", image_id=image_id)


@declare_mcp_resource(uri="/image/png/{image_id}", mime_type="image/png")
def image_png(
    image_id: Annotated[str, Field(description="The unique identifier for the image", min_length=1)],
) -> bytes:
    """Fetch an image by its ID in PNG format."""
    return image(format="png", image_id=image_id)


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
        response, blobs = connection_pool.query(query)
        if not response or not blobs:
            logger.error(f"Image with ID {image_id} not found.")
            raise ValueError(f"Image with ID {image_id} not found.")
    except Exception as e:
        logger.exception(f"Failed to fetch image with ID {image_id}: {e}")
        raise ValueError(f"Failed to fetch image with ID {image_id}: {str(e)}")

    return blobs[0]
