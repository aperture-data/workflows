import time
from typing import List
import requests
import os
from typer import Typer
import datetime

class StatusUpdater:

    def post_update(
            self,
            completed: float,
            phases: List[str],
            phase: str,
            error_message: str,
            error_code: str,
            status: str = "running"):
        try:
            ur = requests.post(
                f"http://{os.environ.get('HOSTNAME')}:8080/response",
                json={
                    "status": status,
                    "completeness": float(completed) / 100.0,
                    "phase": phase,
                    "phases": phases,
                    "accessible": True,
                    "error_message": error_message,
                    "error_code": error_code,
                }
            )

        except Exception as e:
            print(f"Failed to update status: {e}")

class LogProcessor:
    def __init__(self, process: any, phases: List[str] = None, phase: str = "initializing"):
        self.process = process
        self.status_updater = StatusUpdater()
        if not phases:
            phases = ["initializing"]
        self.phases = phases
        self.phase = phase

    def process_logs(self):
        # Continuously read from stderr
        last_time = time.time()
        completed = 100.0
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

            # print("Progress:", line.strip(), flush=True)
            if time.time() - last_time >= 1:
                print(f">>[{datetime.datetime.now().isoformat()}]", msg, flush=True)
                self.status_updater.post_update(
                    completed,
                    self.phases, self.phase,
                    "",
                    "",
                    "running")
                last_time = time.time()
            else:
                print(f"[{datetime.datetime.now().isoformat()}]{msg}", flush=True)

        # Wait for the process to complete
        self.process.wait()

app = Typer()



@app.command()
def shell_updater(
    completed: float = 100.0,
    phases: List[str] = ["initializing"],
    phase: str = "initializing",
    status: str = "running",
    error_message: str = "",
    error_code: str = ""

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