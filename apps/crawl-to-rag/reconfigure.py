import logging
import os


logger = logging.getLogger(__name__)

def reconfigure():
    logger.info("Reconfiguring the application")
    status = "FAILED"
    no_needed_vars = [
        "WF_TOKEN",
        "WF_ALLOWED_ORIGINS",
        "WF_AIMON_API_KEY",
        "WF_AIMON_APP_NAME",
        "WF_AIMON_LLM_MODEL_NAME",
        "WF_LLM_PROVIDER",
        "WF_LLM_API_KEY"
    ]
    try:
        assert os.system(f"cd /workflows/crawl-website && unset WF_INPUT {' '.join(no_needed_vars)} && bash app.sh") == 0
        assert os.system(f"cd /workflows/text-extraction && unset WF_START_URLS WF_ALLOWED_DOMAINS {' '.join(no_needed_vars)} && bash app.sh") == 0
        assert os.system(f"cd /workflows/text-embeddings && unset WF_START_URLS WF_ALLOWED_DOMAINS {' '.join(no_needed_vars)} && bash app.sh") == 0
        status = "SUCCESS"
    except Exception as e:
        logger.error(f"Error reconfiguring the application: {e}")
        raise
    finally:
        return {"message": f"Reconfiguration {status}"}