import subprocess
from log_processor import LogProcessor

command = [f"python3 extract_embeddings.py"]

# Run the command
process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, text=True)

lp = LogProcessor(process)
lp.process_logs()
