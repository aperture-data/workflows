import time
from typing import List
import requests
import os
import sys

import datetime
from status_tools import StatusUpdater
from typer import Typer
import subprocess


class LogProcessor:
    def __init__(self, process: any):
        self.process = process
        self.status_updater = StatusUpdater()

    def process_logs(self):
        # Continuously read from stderr
        last_time = time.time()
        completed = 100.0
        need_update = False
        for line in iter(self.process.stderr.readline, ''):
            msg = line.strip()

            if msg.startswith("Progress:"):
                # Extract the progress percentage from the message
                progress = msg.split(":")[1].strip()
                completed = progress.split("%")[0].strip()
                try:
                    completed = float(completed)
                    need_update = True
                except (ValueError, TypeError):
                    pass

            # print("Progress:", line.strip(), flush=True)
            if need_update and time.time() - last_time >= 1:
                print(f">>[{datetime.datetime.now().isoformat()}]", msg, flush=True)
                self.status_updater.post_update(completed=completed)
                last_time = time.time()
            else:
                print(f"[{datetime.datetime.now().isoformat()}]{msg}", flush=True)

        # Wait for the process to complete
        self.process.wait()



app = Typer()

@app.command()
def run_monitored(command: str):
    process = subprocess.Popen(command, shell=True, stderr=subprocess.PIPE, text=True)
    lp = LogProcessor(process)
    lp.process_logs()

    while process.poll() is None:
        # Process hasn't exited yet, let's wait some
        time.sleep(0.5)

    if process.returncode != 0:
        sys.exit(process.returncode)


if __name__ == "__main__":
    app()
