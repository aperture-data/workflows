import os
import argparse
from prometheus_client import start_http_server
import time
from status import Status
from status_tools import WorkFlowError


workflow_status = Status(phases = ["phase1", "module"])

def main(params):
    workflow_status.status.state("started")
    print("This is the example app...")
    for state in workflow_status.possible_phases[:-1]:
        workflow_status.phases.labels(state)
        for i in range(100):
            workflow_status.phases.labels(state).inc(1/100)
            time.sleep(0.1)
        workflow_status.phases.labels(state).set(1)

    from my_example_module import module_work
    module_work()
    workflow_status.error("An example error occurred", WorkFlowError.WORKFLOW_ERROR)
    time.sleep(10)
    print("Done.")

def get_args():
    obj = argparse.ArgumentParser()

    obj.add_argument('-option0',  type=int,
                     default=os.environ.get('OPTION_0', 100))

    obj.add_argument('-option1',  type=str,
                     default=os.environ.get('OPTION_1', "example"))

    params = obj.parse_args()

    return params

if __name__ == "__main__":
    start_http_server(8001)
    args = get_args()
    main(args)
