# shaper.py - create
import cv2
import numpy as np
import glob
import pickle
import pandas as pd
import numpy as np
import argparse

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument( 'celeb_path' )
    parser.add_argument( '-L','--celeba_limit',default=10000,help="Limit images to process to less than or equal to this number (-1 to disable)")
    parser.add_argument( '-C','--process_count',default=-1)
    parser.add_argument( '-p','--pickle',default="hq.pickle")
    parser.add_argument( '-1','--one_run',action='store_true',help="Don't pickle, everything happens in 1 run")
    parser.add_argument( '-d','--display_bad',action='store_true')
    parser.add_argument( '-S','--save_every',default=250)
    return parser.parse_args()

def single(fn):
    # reading image
    img = cv2.imread(fn)

    # converting image into grayscale image
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # setting threshold of gray image
    _, threshold = cv2.threshold(gray, 127, 255, cv2.THRESH_BINARY)

    # using a findContours() function
    contours, _ = cv2.findContours(
        threshold, cv2.RETR_TREE, cv2.CHAIN_APPROX_SIMPLE)

    i = 0

    cnt = len(contours)
    if cnt < 0 or cnt > 20:
        print(f"WARNING: Unusual count of contours: Found {cnt} contours in {fn}")
    # list for storing names of shapes
    boxes = map(cv2.boundingRect,contours)
    return contours, boxes


def save(opts,store):
    if not opts.one_run:
        with open( opts.pickle,"wb") as f:
            pickle.dump(store,f)
# displaying the image after drawing contours
def main():
    opts = parse_args()

    print(f"processing {opts.celeb_path}")

    store = {}

    try:
        if not opts.one_run:
            with open( opts.pickle,"rb") as f:
                store = pickle.load(f)
    except:
        store = {}


    if not 'mapping' in store:
        import pandas as pd
        df = pd.read_csv( f"{opts.celeb_path}/CelebA-HQ-to-CelebA-mapping.txt",delim_whitespace=True)
        if opts.celeba_limit != -1:
            res = df[df['orig_idx'] <= opts.celeba_limit ]
        else:
            res = df

        store['mapping'] = {}
        for _,celeb_file in res.iterrows():
            cid=celeb_file['idx']
            oid=celeb_file['orig_idx']
            store['mapping'][cid] = { 'oid': oid }
        save(opts,store)

    failed = 0

    procd = 0
    if not 'images' in store:
        print("No processed images")
        store['images'] = {}
        store['bads'] = {}
        store['features'] = {}
    else:
        print("has procd") #print(store['images'])
    parsed = 0
    #for _,celeb_file in res.iterrows():
    for cid,id_data in store['mapping'].items(): #_,celeb_file in res.iterrows():
        #cid=celeb_file['idx']
        #cid=celeb_file['idx']

        print( cid, end=" ")

        if opts.process_count != -1 and procd > int(opts.process_count):
            break

        if cid in store['images']:
            print(f"INFO: Celeb {cid} found, skipping processing")
            procd = procd + 1
            continue
        else:
            store['images'][cid] = {}


        if not opts.one_run and procd != 0 and (procd) % opts.save_every == 0:
            save(opts,store)
            images = len(store['images'])
            print(f"INFO: saved {images} images")


        mask_block=cid//2000

        mask_fn="{}/CelebAMask-HQ-mask-anno/{}/{:05d}*.png".format(opts.celeb_path,mask_block,cid)
        #print(celeb_fn)
        #print(glob.glob(mask_fn))
        #store['images'][cid]['oid'] = oid
        for feat_fn in glob.glob(mask_fn)[:-1]:
            feats = None
            feat_name = "n/a"
            try:
                feat_name = feat_fn.split("/")[-1][6:-4] #.split(".")[0].split("_")[1]
                feats, boxes = single(feat_fn)
                # multiple feats seem to be ok.
                #if len(feats) != 1:
                #	raise Exception(f"ack countour count is {cnt}")
                feat = feats[0]
                # double feature pixel values to map to size
                # for original image ( 512x512 vs 1024x1024 )
                multiplied = []
                for f in feats:
                    m = np.multiply(f,2)
                    multiplied.append(m)
                #print(feat)
                #print(feat_name)
                store['images'][cid][feat_name] = {
                    "polygons": multiplied,
                    "bbox": boxes
                }
                # store['boxes'][cid][feat_name] = boxes


                parsed = parsed + 1
                if not feat_name in store['features']:
                    store['features'][feat_name] = 1
                else:
                    store['features'][feat_name] = store['features'][feat_name] + 1
            except Exception as e :
                if not cid in store['bads']:
                    store['bads'][cid] = {}
                store['bads'][cid][feat_name] = { 'fn': feat_fn, 'err' : str(e) , 'paths':  feats }
                failed = failed + 1
        procd = procd + 1

    print(f"{procd} Celebs :: Parsed {parsed} // Failed {failed}")
    #print(store['features'])

    if not 'fnum' in store:
        store['fnum'] = {}
        for idx,name in enumerate(store['features']):
            print(f"* {name} = {idx}")
            store['fnum'][name] = idx
        save(opts,store)

    generate_adb(store,opts)
    if opts.display_bad:
        for b in store['bads']:
            print("Bad: ",b)
            for btype in store['bads'][b]:
                bdata = store['bads'][b][btype]
                display_image_and_poly( bdata['fn'],btype,bdata['paths'])


def display_image_and_poly(img_path,name,polys):
    # reading image
    img = cv2.imread(img_path)
    cnt = len(polys)
    print(f"Displaying {img_path} of type {name} with {cnt} polys")
    # using drawContours() function
    for idx,poly in enumerate(polys):
            cv2.drawContours(img, [poly], 0, (0, 0, 255), 5)
            # finding center point of shape
            M = cv2.moments(poly)
            x=0
            y=0
            if M['m00'] != 0.0:
                x = int(M['m10']/M['m00'])
                y = int(M['m01']/M['m00'])
            cv2.putText(img, f'{name}_{idx}', (x, y),
                cv2.FONT_HERSHEY_SIMPLEX, 0.6, (255, 255, 255), 2)

            # displaying the image after drawing contours
    cv2.imshow('shapes', img)

    print("waiting...")
    cv2.waitKey(5000)
    print("closing...")
    cv2.destroyAllWindows()
    print("closed...")

def generate_adb(store,opts):

    image_df = pd.DataFrame(columns=['filename','celebahq_id','celeba_id','constraint_celebahq_id'])
    polygon_df = pd.DataFrame(columns=['celebahq_id','celebahqmask_id','constraint_celebahqmask_id','_label','polygons'])
    bboxes_df = pd.DataFrame(columns=['celebahq_id', 'x_pos', 'y_pos', 'width', 'height', 'celebahqbbox_id','constraint_celebahqbbox_id','_label'])

    for chq_id,data in store['images'].items():
        oid = store['mapping'][chq_id]['oid']
        celeb_fn=f"{opts.celeb_path}/CelebA-HQ-img/{chq_id}.jpg"
        image_df.loc[len(image_df.index)] = [ celeb_fn, chq_id ,oid,chq_id ]
        for feat in store['images'][chq_id]:

            # feature id is celeb id with bottom 6 bits for feature id.
            pid = (chq_id << 6) + store['fnum'][feat]
            polygons=""
            try:
                featlist = [ f.tolist() for f in store['images'][chq_id][feat]["polygons"] ]
                prunedlist = []
                # adb will not accept polygons of less than 3 points.
                for idx,sf in enumerate(featlist):
                    # the points we are given are wrapped by
                    # an extra [], so we upwrap here.
                    nf = [ f[0] for f in sf ]
                    if len(nf) < 3:
                        length = len(nf)
                        print(f"not enough points {idx} set for {feat}: size is {length}")
                    else:
                        prunedlist.append(nf)
                polygons = prunedlist
            except:
                raise Exception("Failed to parse for " + celeb_fn+ " feature " + feat + " with " + str(store['images'][chq_id][feat]))
            if len(polygons) == 0 :
                print("WARNING: no polygons for feature {feat} for {celeb_fn}")
            polygon_df.loc[len(polygon_df.index)] =[chq_id, pid,pid,feat,polygons]
            box = store['images'][chq_id][feat]["bbox"]
            x, y, w, h = np.multiply(list(box)[0], 2)
            bboxes_df.loc[len(bboxes_df.index)] = [chq_id, x, y, w, h, pid, pid, feat]

    image_df['id'] = image_df['celebahq_id']
    image_df.to_csv('hqimages.adb.csv',index=False)
    polygon_df.to_csv('hqpolygons.adb.csv',index=False)
    bboxes_df.to_csv('hqbboxes.adb.csv',index=False)

main()
