from contextlib import asynccontextmanager
import uvicorn
from fastapi import FastAPI, Request, BackgroundTasks
import requests
import time
import threading
from prometheus_client.parser import text_string_to_metric_families
import os

import logging
logger = logging.getLogger(__name__)

count = 0
lock = threading.Lock()

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
    RESPONSE["completeness"] = min(count / 10.0, 1.0)  # Simulate progress
    response = None
    try:
        response = requests.get(f"http://{os.environ.get('HOSTNAME')}:{os.environ.get('PROMETHEUS_PORT')}/")
    except requests.exceptions.ConnectionError:
        # This is not an error. The workflows might not be running yet.
        # Or they might not have implemented the prometheus endpoint yet.
        logger.info("Could not connect to the server. The server might not be running.")
    except Exception as e:
        RESPONSE["accessible"] = False
        RESPONSE["error_message"] = str(e)
        RESPONSE["error_code"] = "workflow_error"
        return
    if response and response.ok and response.text:
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
            ur = requests.post(f"http://{os.environ.get('HOSTNAME')}:{os.environ.get('PORT')}/response", json=RESPONSE)
            if ur.ok:
                print("Status updated successfully.")
        except Exception as e:
            print(f"Failed to update status: {e}")

running = True
def get_status_periodically():
    """
    Fetches the status periodically every 10 seconds.
    """
    global running
    while running:
        get_workflow_status()
        time.sleep(2)

# This block allows you to run the FastAPI application directly
# when the script is executed.
@asynccontextmanager
async def lifespan(app: FastAPI):
    global running
    background_thread = threading.Thread(target=get_status_periodically)
    background_thread.daemon = True  # Allows the thread to exit when the main program exits
    #Commented since we don't want to fetch status from the server till atleast 1 workflow implements it.
    background_thread.start()  # Start the background thread to fetch status
    yield
    running = False
    background_thread.join()
    print("Status server stopped.")

# Initialize the FastAPI application
app = FastAPI(
    title="Workflows Status Server",
    description="A simple server to manage and report the status of workflows.",
    version="0.2.0",
    lifespan=lifespan
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

def reconfigure_workflow():
    from reconfigure import reconfigure
    retval = reconfigure()
    return retval

@app.post(
    "/reconfigure",
    summary="Reconfigures the application.",
    response_description="Confirmation that the application has been reconfigured."
)
async def reconfigure(request: Request, background_tasks: BackgroundTasks):
    """
    Reconfigures the application.
    """
    logger.info(f"Reconfiguring the application with request: {request}")
    background_tasks.add_task(reconfigure_workflow)
    return {"message": "Reconfiguration requested."}

@app.post(
    "/response",
    summary="Set response data",
    response_description="Confirmation that the response data has been set."
)
async def set_response(request: Request):
    """
    Sets the global RESPONSE dictionary with the provided data.

    This endpoint allows you to update the RESPONSE dictionary,
    which can be used in other parts of the application.
    """
    req_json = await request.json()
    print(f"{req_json=}")
    global RESPONSE
    with lock:
        for key, value in req_json.items():
            RESPONSE[key] = value
    return {"message": "Response data set successfully"}



if __name__ == "__main__":
    # Runs the Uvicorn server.
    # 'main:app' tells Uvicorn to look for an 'app' object in 'main.py'.
    # '--reload' enables auto-reloading when code changes are detected.
    uvicorn.run(
        "status_server:app",
        host="0.0.0.0",
        port=int(os.environ.get('PORT')),
        reload=False,
        access_log=False,
        timeout_keep_alive=1800)
