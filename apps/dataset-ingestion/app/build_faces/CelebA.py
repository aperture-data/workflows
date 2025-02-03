import os
import pandas as pd
import time
import argparse

from aperturedb.Subscriptable import Subscriptable

class CelebA(Subscriptable):
    def __init__(self, cli_args):
        df = pd.read_csv(cli_args.attr_file, delim_whitespace=True, header=1)
        df_bbox = pd.read_csv(cli_args.bbox_file, delim_whitespace=True, header=1)

        df = pd.merge(left=df, right=df_bbox, left_index=True, right_on="image_id")
        self.collection = df.to_dict(orient="records")
        self.images_align_root = cli_args.images_align_root

    def __len__(self):
        return len(self.collection)

    def getitem(self, index):
        p = self.collection[index]
        p["crop"] = False
        q = [
            {
                "AddImage": {
                    "_ref": 1,
                    "if_not_found": {
                        "image_id": ["==", p["image_id"]],
                        "crop": ["==", False]
                    },
                    "properties": {
                        c: p[c] for c in p.keys()
                    },
                }
            }
        ]

        img_align_path = os.path.join(self.images_align_root, p["image_id"])
        blob_align = open(img_align_path, "rb").read()

        return q, [blob_align]

    def write_csv(self, filename):
        print("Processing crop images")

        df = pd.DataFrame(self.collection)

        df ['filename'] = df['image_id'].apply(lambda x: os.path.join(self.images_align_root, x))
        df ['image_id'] = df['image_id'].apply(lambda x: f"cropped_{os.path.splitext(x)[0]}")
        df ['constraint_image_id'] = df['image_id']
        cols = df.columns.tolist()
        cols.remove('filename')
        cols.insert(0, 'filename')
        df = df[cols]


        df.to_csv(filename, index=False)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-R", "--images_align_root", type=str,
                        default="data", help="Images root directory")
    parser.add_argument("-A", "--attr_file", type=str,
                        default="input/list_attr_celeba.txt", help="Attribute file")
    parser.add_argument("-B", "--bbox_file", type=str,
                        default="input/list_bbox_celeba.txt", help="Bounding box file")
    return parser.parse_args()

if __name__ == "__main__":
    start = time.time()
    cli_args = parse_args()
    celebA = CelebA(cli_args)
    celebA.write_csv("celebA.csv")
    print("Done")
    print("Time: ", time.time() - start)