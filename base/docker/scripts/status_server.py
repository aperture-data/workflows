import uvicorn
from fastapi import FastAPI
import requests
import  time
import threading
from prometheus_client.parser import text_string_to_metric_families
import os

count = 0

RESPONSE = {
    "status": "starting",  # Not defined yet.
    "completeness": 0.0, #Completeness of phase (0.0 to 1.0)
    "phase": "initializing",  # Initial phase
    "phases": ["initializing"], # possible phases of the workflow
    "accessible": False,  # Whether the workflow is accessible
    "error_message": "", # Error message if any
    "error_code": "" # ("db_error", "workflow_error", "api_error")
}

def get_workflow_status():
    global count
    count += 1
    global RESPONSE
    # print("Fetching status from the server...")
    RESPONSE["completeness"] = count / 10.0  # Simulate progress
    try:
        response = requests.get(f"http://{os.environ.get('HOSTNAME')}:8000")
    except Exception as e:
        # print("Connection refused. The server might not be running.")
        RESPONSE["accessible"] = False
        RESPONSE["error_message"] = str(e)
        RESPONSE["error_code"] = "workflow_error"
        return
    if response.ok and response.text:
        print("Success!")
        for metric in text_string_to_metric_families(response.text):

            for sample in metric.samples:
                if sample.name == "workflow_information_info":
                    RESPONSE["phases"] = sample.labels.get("phases", "").split("|")
                    RESPONSE["error_message"] = sample.labels.get("error_message", "")
                    RESPONSE["error_code"] = sample.labels.get("error_code", "")
                if sample.name == "status":
                    RESPONSE["status"] = sample.labels.get("status", "unknown")
                if sample.name == "phases":
                    RESPONSE["phase"] = sample.labels.get("phase", "unknown")
                    RESPONSE["completeness"] = sample.value
                if sample.name == "accessible":
                    if sample.labels.get("accessible") == "yes":
                        RESPONSE["accessible"] = sample.value == 1.0


        try:
            ur = requests.post(f"http://{os.environ.get('HOSTNAME')}:8080/response", json=RESPONSE)
            if ur.ok:
                print("Status updated successfully.")
        except Exception as e:
            print(f"Failed to update status: {e}")


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
    global RESPONSE
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
    for key, value in response.items():
        RESPONSE[key] = value

# This block allows you to run the FastAPI application directly
# when the script is executed.
if __name__ == "__main__":
    def get_status_periodically():
        """
        Fetches the status periodically every 10 seconds.
        """
        while True:
            get_workflow_status()
            time.sleep(2)
    background_thread = threading.Thread(target=get_status_periodically)
    background_thread.daemon = True  # Allows the thread to exit when the main program exits
    background_thread.start()  # Start the background thread to fetch status

    # Runs the Uvicorn server.
    # 'main:app' tells Uvicorn to look for an 'app' object in 'main.py'.
    # '--reload' enables auto-reloading when code changes are detected.
    uvicorn.run("status_server:app", host="0.0.0.0", port=8080, reload=True, access_log=False)
