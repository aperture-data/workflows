# loader.py - Load CelebA-HQ results into ApertureDB

import os
import pickle
import pandas as pd
from aperturedb.CommonLibrary import create_connector

import argparse
import logging

logging.basicConfig(level=logging.DEBUG)

def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument( '-s','--server',required=True)
	return parser.parse_args()

def retrieve_image_ids(db):

    if os.path.exists( "celebaids.pickle"):
            with open( "celebaids.pickle","rb") as fp:
                    return pickle.load(  fp )

    chunk_size = 500
    offset = 0

    # find image count, set up progress bar.
    query = [{"FindImage": {
                "blobs":False,
                "constraints": {
                        "image_id" : ["!=",None]
                },
                "results": {
                    "count":True
                    }
                }
            }]
    result,blobs = db.query(query)
    total_images = result[0]["FindImage"]["count"]
    print("Total CelebA images = ",total_images)

    # change query to retrieve image_id
    query[0]["FindImage"]["results"]["list"] = ["image_id"]
    del query[0]["FindImage"]["results"]["count"]


    image_map = {}
    while offset < total_images:
        # modify query template. Limit by image count.
        query[0]["FindImage"]["offset"] = offset
        query[0]["FindImage"]["limit"] = chunk_size

        result,blobs = db.query(query)
        #bar.update(min(total_images,offset+chunk_size))

        if isinstance( result, dict ):
            print(f"query failed: {result}")
            return None
        else:
            # create map from id to unique id to image guid
            img_info = result[0]["FindImage"]["entities"]
            def map_ids( idlist ):
                    return map( lambda ele: ele["image_id"],  idlist )
            try:
                    for item in map_ids(img_info):
                            base_id = int(item.split("_")[1]) -1
                            if base_id not in image_map:
                                    image_map[base_id] = []
                            image_map[base_id].append(item)

		    #new_items = { int(item.split("_")[1]): item for item in map_ids(img_info) }
            except Exception as e:
                    print("Failed split ", result)
                    return None
	    #image_map.update( new_items )

        offset = offset + chunk_size

    with open( "celebaids.pickle","wb") as fp:
            pickle.dump(image_map,fp)

    return image_map

def create_connections(server_id_map):
        images_df = pd.read_csv( "images.adb.csv")
        connections_df = pd.DataFrame(columns=['ConnectionClass','_Image@celebahq_id','_Image@image_id',\
                                                        "connect_id","constraint_connect_id"])
        for i in range(len(images_df)):
                chq_id = images_df.loc[i,"celebahq_id"]
                celeba_id = images_df.loc[i,"celeba_id"]
                celeba_links = server_id_map[celeba_id]
                celeba_link = None
                if any( map( lambda e: e.startswith("cropped_"), celeba_links)):
                        celeba_link = next(filter( lambda e: e.startswith("cropped_"),celeba_links))
                elif any( map( lambda e: e.startswith("full_"), celeba_links)):
                        celeba_link = next(filter( lambda e: e.startswith("full_"),celeba_links))
                else:
                        celeba_link = celeba_links[0]
                connections_df.loc[len(connections_df.index)] =\
                                        [ "CelebAHQToCelebA", chq_id ,celeba_link,chq_id,chq_id]

        connections_df.to_csv("connections.adb.csv",index=False)

# we need to findimage with returning image_id
if __name__ == '__main__':
	opts = parse_args()

	if not os.path.exists("images.adb.csv"):
		print("Missing images csv.")
		sys.exit(1)

	os.environ["APERTUREDB_CONFIG"] = opts.server
	c = create_connector()
	server_id_map = retrieve_image_ids(c)

	create_connections( server_id_map )
