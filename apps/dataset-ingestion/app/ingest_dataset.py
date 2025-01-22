import os
import argparse

<<<<<<< HEAD
=======
from aperturedb import Utils

>>>>>>> 9552f0c (automatic commit message)
def main(params):

    print("This is the example app...")
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
    args = get_args()
    main(args)
