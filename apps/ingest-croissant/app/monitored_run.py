import subprocess
from log_processor import LogProcessor

command = [f"/app/venv/bin/adb ingest from-croissant $WF_CROISSANT_URL $COMMAND_ARGS"]

# Run the command
process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, text=True)

lp = LogProcessor(process)
lp.process_logs()
