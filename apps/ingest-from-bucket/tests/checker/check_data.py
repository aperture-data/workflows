# check_data.py
from argparse import ArgumentParser
from aperturedb.CommonLibrary import create_connector

def get_args():
    parser = ArgumentParser()
    parser.add_argument("--images",type=int,required=True)
    parser.add_argument("--videos",type=int,required=True)
    parser.add_argument("--pdfs",type=int,required=True)

    opts = parser.parse_args()
    return opts

def check_count_in_db(client, images:int, videos:int, pdfs:int ):
    cmds = { "Image": images,
            "Video": videos,
            "Blob": pdfs
        }

    for cmd in cmds.keys():
        query = [{
            f"Find{cmd}": {
                "results": {
                    "count":True
                }
            }
            }]
        res,_ = client.query(query)
        if not isinstance(res,list):
            raise Exception(f"Query for {cmd} failed: {res}")
        cnt = res[0][f"Find{cmd}"]["count"]
        if cnt != cmds[cmd]:
            raise Exception(f"Expected {cmds[cmd]} in db, got {cnt} for {cmd}")
        else:
            print(f"Successfully found {cnt} for {cmd}")

if __name__ == '__main__':
    args = get_args()
    db = create_connector()
    check_count_in_db(client=db, images=args.images, videos=args.videos, pdfs=args.pdfs)
