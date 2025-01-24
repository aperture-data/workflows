import os
from aperturedb.Subscriptable import Subscriptable
import pandas as pd
import time


class CelebA(Subscriptable):
    def __init__(self,
                 attr_file:str = "input/list_attr_celeba.txt",
                 bbox_file:str = "input/list_bbox_celeba.txt",
                 landmarks_file:str = "input/image_data/CelebA/Anno/list_landmarks_celeba.txt",):
        attr_file = attr_file
        df = pd.read_csv(attr_file, delim_whitespace=True, header=1)

        bbox_file = bbox_file
        df_bbox = pd.read_csv(bbox_file, delim_whitespace=True, header=1)

        # landmarks_file = landmarks_file
        # df_landmarks = pd.read_csv(landmarks_file, delim_whitespace=True, header=1)

        df = pd.merge(left=df, right=df_bbox, left_index=True, right_on="image_id")
        # df = pd.merge(left=merge1, right=df_landmarks, right_index=True, left_on="image_id")

        self.collection = df.to_dict(orient="records")

        # self.images_root = "input/images/img_celeba"
        self.images_align_root = "input/images/img_align_celeba"


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


if __name__ == "__main__":
    start = time.time()
    celebA = CelebA()
    celebA.write_csv("celebA.csv")
    print("Done")
    print("Time: ", time.time() - start)