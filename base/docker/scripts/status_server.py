import uvicorn
from fastapi import FastAPI

RESPONSE = {
    "status": 1,  # Not defined yet.
    "completeness": 0.2,
    "phase": "mock2",
    "phases": ["mock1", "mock2", "mock3"],
    "accessible": True,
    "error_message": "no error, but this is a mock response",
    "error_code": "no_error_because_this_is_a_mock_response"
}

# Initialize the FastAPI application
app = FastAPI(
    title="Workflows Status Server",
    description="A simple server to manage and report the status of workflows.",
    version="0.1.0"
)


@app.get(
    "/status",
    summary="Get workflow status",
    response_description="Current status of the workflow, including completeness, phase, and error messages."
)
async def get_status():
    """
    Returns the current operational status of the application.

    This endpoint provides detailed information about the application's state,
    its completeness, current phase, and any error messages.
    """
    return RESPONSE


@app.post(
    "/response",
    summary="Set response data",
    response_description="Confirmation that the response data has been set."
)
async def set_response(response: dict):
    """
    Sets the global RESPONSE dictionary with the provided data.

    This endpoint allows you to update the RESPONSE dictionary,
    which can be used in other parts of the application.
    """
    global RESPONSE
    RESPONSE = response


@app.get(
    "/response",
    summary="Get response data",
    response_description="Current response data stored in the global RESPONSE dictionary."
)
async def get_response():
    """
    Retrieves the current RESPONSE dictionary.

    This endpoint returns the data stored in the global RESPONSE dictionary,
    which can be set via the /response POST endpoint.
    """
    return RESPONSE


# This block allows you to run the FastAPI application directly
# when the script is executed.
if __name__ == "__main__":
    # Runs the Uvicorn server.
    # 'main:app' tells Uvicorn to look for an 'app' object in 'main.py'.
    # '--reload' enables auto-reloading when code changes are detected.
    uvicorn.run("status_server:app", host="0.0.0.0", port=8080, reload=True)
