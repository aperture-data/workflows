from status import Status
import time

def module_work():
    status = Status()
    status.accesssible.state("yes")
    for i in range(100):
        status.phases.labels("module").set(i/100)
        time.sleep(0.2)

