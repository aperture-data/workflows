"""Site customization module for setting up global exception handling."""
import sys
import logging

from status_tools import StatusUpdater, WorkFlowError


old_handler = sys.excepthook

logging.info("Setting up exception handler")
updater = StatusUpdater()

def exception_handler(etype, value, tb):
    """Handle uncaught exceptions by posting status updates."""
    updater.post_update(
        error_message=f"Exception: {etype.__name__} {value}",
        error_code=WorkFlowError.WORKFLOW_ERROR
    )
    old_handler(etype, value, tb)

sys.excepthook = exception_handler
