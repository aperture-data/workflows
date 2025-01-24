import hashlib
import pandas as pd
import sys
import os
from tqdm import tqdm

if __name__ == "__main__":
    tqdm.pandas()
    df = pd.read_csv(sys.argv[1])
    filename = os.path.basename(sys.argv[1])

    def gen_sha(filename):
        with open(filename, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()

    #generate column
    df = df.assign(sha = lambda x: x.filename)
    df["sha"] = df["sha"].progress_apply(gen_sha)

    df = df.drop_duplicates(subset=["sha"])
    df = df.drop(columns=["sha"])
    df.to_csv(f"pruned_{filename}", index=False)