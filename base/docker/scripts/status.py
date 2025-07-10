from typing import List, Optional
from prometheus_client import Gauge, Enum, Info
import logging
from enum import Enum as PyEnum
from typer import Typer
import os
import requests

logger = logging.getLogger(__name__)

class WorkflowStatus(PyEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "completed"
    FAILED = "failed"

class WorkFlowError(PyEnum):
    DB_ERROR = "db_error"
    WORKFLOW_ERROR = "workflow_error"
    API_ERROR = "api_error"

def singleton(cls):
    instances = {}
    def get_instance(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]
    return get_instance

class StatusUpdater:
    def post_update(
            self,
            completed: Optional[float] = None,
            phases: Optional[List[str]] = None,
            phase: Optional[str] = None,
            error_message: Optional[str] = None,
            error_code: Optional[WorkFlowError] = None,
            status: WorkflowStatus = WorkflowStatus.RUNNING):
        wf_status = {
        }
        if completed is not None:
            wf_status["completeness"] = float(completed) / 100.0
        if phases is not None:
            wf_status["phases"] = phases
        if phase is not None:
            wf_status["phase"] = phase
        if error_message is not None:
            wf_status["error_message"] = error_message
        if error_code is not None:
            wf_status["error_code"] = error_code.value
        if status is not None:
            wf_status["status"] = status.value
        if len(wf_status) > 0:
            print(f"Updating status: {wf_status}")
            try:
                update_response = requests.post(
                        f"http://{os.environ.get('HOSTNAME')}:8080/response",
                        json=wf_status
                    )
            except Exception as e:
                print(f"Failed to update status: {e}")


@singleton
class Status:
    accesssible = Enum("accessible", "Is the workflow accessible?", states=["yes", "no"])
    status = Enum("status", "Status of the workflow", states=["started"])
    phases = Gauge("phases", "Phases of the workflow", ["phase"])
    workflow_info = Info("workflow_information", "Information about the state of the workflow")

    def __init__(self, phases: List[str]):
        assert isinstance(phases, list), "phases must be a list"
        assert len(phases) > 0, "phases list cannot be empty"
        self.possible_phases = phases
        self.description = {
            "version": "1.0.0",
            "description": "This is an example workflow application.",
            "phases": "|".join(phases),
            "error_message": "",
            "error_code": "",
        }
        self.workflow_info.info(self.description)
        self.accesssible.state("no")
        logger.info("Workflow Status created.")

    def error(self, message: str, code: WorkFlowError):
        """
        Set an error message and code for the workflow status.
        """
        self.description["error_message"] = message
        self.description["error_code"] = code.value
        self.workflow_info.info(self.description)
        logger.error(f"Error in workflow: {message} (Code: {code})")

app = Typer()

@app.command()
def shell_updater(
    completed: Optional[float] = None,
    phases: Optional[List[str]] = None,
    phase: Optional[str] = None,
    status: WorkflowStatus = WorkflowStatus.RUNNING,
    error_message: Optional[str] = None,
    error_code: Optional[WorkFlowError] = None,
):
    ssu = StatusUpdater()
    ssu.post_update(
        completed=completed,
        phases=phases,
        phase=phase,
        status=status,
        error_message=error_message,
        error_code=error_code)

if __name__ == "__main__":
    app()