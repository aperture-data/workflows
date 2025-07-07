import time
import requests
import os

class LogProcessor:
    def __init__(self, process: any):
        self.process = process

    def process_logs(self):
        # Continuously read from stderr
        last_time = time.time()
        for line in iter(self.process.stderr.readline, ''):
            msg = line.strip()

            if msg.startswith("sample_count"):
                # Extract the sample count from the message
                sample_count = msg.split(":")[1].strip()
                completed = 0
            if msg.startswith("Progress:"):
                # Extract the progress percentage from the message
                progress = msg.split(":")[1].strip()
                completed = progress.split("%")[0].strip()
                # Print the progress message
            # print("Progress:", line.strip(), flush=True)
            try:
                if time.time() - last_time < 1:
                    continue
                print("Posting status update:", msg, flush=True)
                ur = requests.post(
                    f"http://{os.environ.get('HOSTNAME')}:8080/response",
                    json={
                        "status": "running",
                        "completeness": float(completed) / 100.0,
                        "phase": "ingesting",
                        "phases": ["ingesting"],
                        "accessible": True,
                        "error_message": "",
                        "error_code": ""
                    }
                )
                last_time = time.time()
            except Exception as e:
                print(f"Failed to update status: {e}")

        # Wait for the process to complete
        self.process.wait()
