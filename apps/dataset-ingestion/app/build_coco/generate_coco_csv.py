import argparse
import os
import random

import pandas as pd
import numpy as np

from datetime import datetime

from pycocotools.coco import COCO

RANDOM_SEED = 42

def yfcc_url_to_id(url):

    return url.split("/")[-1].split("_")[0]

def date_to_iso(date):

    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').isoformat()

def generate_image_csv(imgs, im_list, path, op_path, stage, coco, conditional):

    df = pd.DataFrame()
    df["filename"]   = [ path + "/" + imgs[im]["file_name"] for im in im_list ]
    df["id"]         = [ imgs[im]["id"]         for im in im_list ]
    df["file_name"]  = [ imgs[im]["file_name"]  for im in im_list ]
    df["height"]     = [ imgs[im]["height"]     for im in im_list ]
    df["width"]      = [ imgs[im]["width"]      for im in im_list ]
    df["license"]    = [ imgs[im]["license"]    for im in im_list ]
    df["coco_url"]   = [ imgs[im]["coco_url"]   for im in im_list ]
    df["flickr_url"] = [ imgs[im]["flickr_url"] for im in im_list ]
    df["date:date_captured"] = [ date_to_iso(imgs[im]["date_captured"])
                                 for im in im_list]
    df["aspect_ratio"]       = [ imgs[im]["width"] / imgs[im]["height"]
                                 for im in im_list ]
    df["yfcc_id"]       = df["flickr_url"].map(yfcc_url_to_id)
    df["type"]          = df["id"].map(lambda x: stage)
    df["corpus"]        = df["id"].map(lambda x: stage)
    if conditional:
        df["constraint_id"] = df["id"]

    img_csv_fname = os.path.join(op_path, stage + "_images.adb.csv")
    df.to_csv(img_csv_fname, index=False)

    return df["id"], df["yfcc_id"]

def generate_pixel_mask_csv(imgs, seg_path, op_path, image_ids, stage, conditional):

    seg1_dfs = pd.DataFrame()
    seg1_dfs["filename"] = [ seg_path + imgs[im]["file_name"].split('.')[0] + ".png"
                             for im in image_ids ]
    seg1_dfs["seg_id"]            = image_ids
    if conditional:
        seg1_dfs["constraint_seg_id"] = image_ids
    seg1_dfs["corpus"]            = [stage for x in image_ids]

    fname = os.path.join(op_path, stage + "_pixelmaps.adb.csv")
    seg1_dfs.to_csv(fname, index=False)

    img_seg1_conn_dfs = pd.DataFrame()
    img_seg1_conn_dfs["ConnectionClass"] = ["segmentation" for x in image_ids]
    img_seg1_conn_dfs["_Image@id"]       = image_ids
    img_seg1_conn_dfs["_Image@seg_id"]   = image_ids
    img_seg1_conn_dfs["id"]              = image_ids
    if conditional:
        img_seg1_conn_dfs["constraint_id"]   = image_ids
    img_seg1_conn_dfs["corpus"]          = [stage for x in image_ids]

    fname = os.path.join(op_path, stage + "_img_pixelmap_connections.adb.csv")
    img_seg1_conn_dfs.to_csv(fname, index=False)

def check_bbox_coord(val):

    return int(val) if int(val) > 0 else 1

def generate_bbox_csv(imgs, seg_path, op_path, image_ids, seg2_id, stage, coco, conditional):

    bbox_dfs = pd.DataFrame()

    anns = [ annotations for img_id      in image_ids
                         for annId       in coco.getAnnIds(imgIds=[img_id])
                         for annotations in coco.loadAnns([annId])
            ]

    total_area = [ imgs[img_id]["width"] * imgs[img_id]["height"]
                   for img_id in image_ids
                   for i in coco.getAnnIds(imgIds=[img_id]) ]

    bbox_dfs["id"]          = [ img_id for img_id in image_ids
                                       for i in coco.getAnnIds(imgIds=[img_id]) ]
    bbox_dfs["x_pos"]       = [ check_bbox_coord(ann["bbox"][0]) for ann in anns ]
    bbox_dfs["y_pos"]       = [ check_bbox_coord(ann["bbox"][1]) for ann in anns ]
    bbox_dfs["width"]       = [ check_bbox_coord(ann["bbox"][2]) for ann in anns ]
    bbox_dfs["height"]      = [ check_bbox_coord(ann["bbox"][3]) for ann in anns ]
    bbox_dfs["bbox_id"]     = [ ann["id"]          for ann in anns]
    if conditional:
        bbox_dfs["constraint_bbox_id"]     = [ ann["id"]          for ann in anns]
    bbox_dfs["image_id"]    = [ ann["image_id"]    for ann in anns]
    bbox_dfs["category_id"] = [ ann["category_id"] for ann in anns]
    bbox_dfs["area"]        = [ ann["area"] for ann in anns]
    bbox_dfs["area_perc"]   = [ ann["area"] / t_area
                                for (ann, t_area) in zip(anns, total_area) ]
    bbox_dfs["is_crowd"]    = [ ann["iscrowd"] for ann in anns]
    bbox_dfs["_label"]      = [ coco.loadCats(ids=ann["category_id"])[0]["name"]
                                   for ann in anns ]
    bbox_dfs["corpus"]      = [stage for ann in anns]

    fname = os.path.join(op_path, stage + "_bboxes.adb.csv")
    bbox_dfs.to_csv(fname, index=False)

def _coco_polygon_to_adb_polygon(coco_poly, offset=(0.0,0.0)):
    return [[coco_poly[i]+offset[0],coco_poly[i+1]+offset[1]] for i in range(0,len(coco_poly),2)]

def _coco_segmentation_to_adb_polygons(coco_seg, offset=(0.0,0.0)):
    return [_coco_polygon_to_adb_polygon(coco_poly,offset) for coco_poly in coco_seg]

def generate_polygon_csv(op_path, image_ids, stage, coco, conditional):

    SELF_INTERSECTING_IDS = [
        112058,439437,1739161,1746989,1889299,1903692,
        43101,66776,72306,81707,84930,100074,106259,128551,129193,138704,175423,206849,211109,217875,264532,265143,458930,497030,512331,710934,1053842,1258446,1271939,1301323,1818283,
    ]

    EMPTY_IDS = [
        918,2206849
    ]

    anns = [ annotations for img_id      in image_ids
                         for annId       in coco.getAnnIds(imgIds=[img_id])
                         if annId not in SELF_INTERSECTING_IDS and annId not in EMPTY_IDS
                         for annotations in coco.loadAnns([annId])
                         if annotations["iscrowd"] == 0
            ]

    img_ids = [ ann["image_id"] for ann in anns ]
    ann_ids = [ ann["id"] for ann in anns ]

    polygon_dfs = pd.DataFrame()
    polygon_dfs["id"]                   = img_ids
    polygon_dfs["image_id"]             = img_ids
    polygon_dfs["ann_id"]               = ann_ids
    if conditional:
        polygon_dfs["constraint_ann_id"]    = ann_ids
    polygon_dfs["category_id"]          = [ ann["category_id"]  for ann in anns]
    polygon_dfs["_label"]               = [
        coco.loadCats(ids=ann["category_id"])[0]["name"] for ann in anns ]
    polygon_dfs["corpus"]               = [ stage for _ in anns ]
    polygon_dfs["polygons"]             = [
        _coco_segmentation_to_adb_polygons(ann["segmentation"]) for ann in anns ]

    fname = os.path.join(op_path, stage + "_polygons.adb.csv")
    polygon_dfs.to_csv(fname, index=False)

    return ann_ids

def generate_descriptors_csv(descriptors_bin, op_path, stage, yfcc_ids, conditional):

    dtype = [('id', np.int64), ('descs', np.float32, (4096))]

    with open(descriptors_bin, 'rb') as fh:
        full_data = np.fromfile(fh, dtype)

    yfcc_set = {np.int64(id) for id in yfcc_ids}
    data = full_data[[id in yfcc_set for id in full_data["id"]]]

    print("Descriptors shape:", (data["descs"]).shape)

    numpy_file = os.path.join(op_path, stage + "_descriptors.npy")
    np.save(numpy_file, data["descs"])

    # Descriptors

    df = pd.DataFrame()

    df["filename"]  = [numpy_file for i in data["id"]]
    df["index"]     = [i for i in range(len(data["id"]))]
    df["set"]       = ["coco_descriptors" for i in data["id"]]
    df["yfcc_id"]             = data["id"]
    if conditional:
        df["constraint_yfcc_id"]  = data["id"]
    df["corpus"]              = [stage for _ in data["id"]]

    df.to_csv( os.path.join(op_path, stage + "_descriptors.adb.csv") , index=False)

    # Connections

    df = pd.DataFrame()

    df["ConnectionClass"]     = ["has_descriptor" for i in data["id"]]
    df["_Image@yfcc_id"]      = data["id"]
    df["_Descriptor@yfcc_id"] = data["id"]
    df["yfcc_id"]             = data["id"]
    if conditional:
        df["constraint_yfcc_id"]  = data["id"]
    df["corpus"]              = [stage for _ in data["id"]]

    df.to_csv(os.path.join(op_path, stage + "_connections.adb.csv") , index=False)

    return


def main(args):

    my_date = datetime.now()
    print(my_date.isoformat())
    print(f"Using parameters {args}")

    in_path   = args.input_file_path
    op_path   = args.output_file_path
    desc_file = args.descriptors_bin
    stages    = args.stages.split(',')

    for stage in stages:

        metadata_file   = in_path + "/annotations/instances_{}2017.json".format(stage)
        images_path     = in_path + "/images/{}2017/".format(stage)
        images_seg_path = in_path + "/images/stuff_{}2017_pixelmaps/".format(stage)

        coco = COCO(metadata_file)
        imgs = coco.imgs

        print("Writing data for stage \"{}\" ({} images)".format(stage, len(imgs)))

        im_list = [im for im in imgs]

        if args.sample is not None:
            im_list = random.Random(RANDOM_SEED).sample(im_list, int(len(im_list) * args.sample / 100))

        print("Creating images...")
        image_ids, yfcc_ids = generate_image_csv(imgs, im_list, images_path, op_path, stage, coco, args.conditional)
        print("Creating pixelmaps...")
        generate_pixel_mask_csv(imgs, images_seg_path, op_path, image_ids, stage, args.conditional)
        print("Creating bboxes...")
        generate_bbox_csv(imgs, images_seg_path, op_path, image_ids, 1, stage, coco, args.conditional)
        print("Creating polygons...")
        generate_polygon_csv(op_path, image_ids, stage, coco, args.conditional)
        if args.generate_embeddings:
            print("Creating descriptors...")
            generate_descriptors_csv(desc_file, op_path, stage, yfcc_ids, args.conditional)

    print("Finished all stages")

    print("Done")

def get_args():
    # https://stackoverflow.com/a/43357954
    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    obj = argparse.ArgumentParser()

    # Input CSV
    obj.add_argument('-input_file_path',  type=str, default="data/")
    obj.add_argument('-output_file_path', type=str, default="data/")

    obj.add_argument('-descriptors_bin',  type=str,
                                          default="data/coco_descriptors.bin")

    obj.add_argument('-stages', type=str, default=os.environ.get('STAGES', "val,train"))
    obj.add_argument('-sample', type=int, default=os.environ.get('SAMPLE'))

    obj.add_argument('-conditional', type=str2bool, default=os.environ.get('CONDITIONAL', True))
    obj.add_argument('-generate_embeddings', type=str2bool, default=True)



    params = obj.parse_args()

    if params.sample == 100:
        params.sample = None

    return params

if __name__ == "__main__":
    args = get_args()
    print(args)
    main(args)
