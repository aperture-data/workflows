import sys
from status import StatusUpdater, WorkFlowError, WorkflowStatus

old_handler = sys.excepthook

print("Setting up exception handler")
updater = StatusUpdater()

def exception_handler(type, value, tb):
    updater.post_update(
        error_message=f"Exception: {type.__name__} {value}",
        error_code=WorkFlowError.WORKFLOW_ERROR
    )
    old_handler(type, value, tb)

sys.excepthook = exception_handler
