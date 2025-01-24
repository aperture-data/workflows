from contextlib import redirect_stderr, redirect_stdout

import pandas as pd

from datetime import datetime

from aperturedb import VideoDownloader
from aperturedb import EntityDataCSV, ConnectionDataCSV, VideoDataCSV, ParallelLoader
import os


def url_to_name(url):
    return "sample_data/videos/" + url.split("/")[-1]

def url_to_guid(url):
    # drop the format (mp4 usually)
    return url.split("/")[-1].split(".")[0]

# Madeup date property
def get_date(x):
    now = datetime.now()
    return now.isoformat()

def generate_video_csv(input_file, output_path):
    urls_df = pd.read_csv(input_file)

    urls = urls_df["url"]

    df = pd.DataFrame()
    df["filename"] = urls.map(url_to_name)
    df["url"]      = urls
    df["guid"]     = urls.map(url_to_guid)
    df["sequence"] = [x for x in range(len(urls))]
    df["date:date_captured"] = urls.map(get_date)
    df["constraint_guid"]    = df["guid"]
    df["adb_data_source"]    = "yfcc100"

    df = df.reindex(columns=["filename", "url", "guid", "sequence",
                             "date:date_captured", "constraint_guid", "adb_data_source"])

    img_csv_fname = output_path + "/" + "videos.adb.csv"
    df.to_csv(img_csv_fname, index=False)

    return


def download_videos(in_csv_file, error_file_path):
    loader = VideoDownloader.VideoDownloader()
    loader.check_video = True
    num_threads = 32
    dest_dir = os.path.dirname(error_file_path)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    with open(error_file_path, 'w') as f:
        with redirect_stderr(f):
            loader.run(
                generator=VideoDownloader.VideoDownloaderCSV(in_csv_file),
                numthreads=num_threads,
                batchsize=10,
                stats=True)

def load_all(db, input_file_path, batchsize, numthreads, error_file_path):
    stats      = True
    loader = ParallelLoader.ParallelLoader(db)
    loading_info = [
        ("Loading videos ...", VideoDataCSV.VideoDataCSV, "videos/videos.adb.csv"),
        ("Loading locations", EntityDataCSV.EntityDataCSV, "example_location_metadata.adb.csv"),
        ("Connecting videos and locations", ConnectionDataCSV.ConnectionDataCSV, "example_location_video_connections.adb.csv"),
        ("Loading cameras", EntityDataCSV.EntityDataCSV, "example_camera_metadata.adb.csv"),
        ("Connecting videos and cameras", ConnectionDataCSV.ConnectionDataCSV, "example_camera_video_connections.adb.csv"),
        ("Loading stores", EntityDataCSV.EntityDataCSV, "example_store_metadata.adb.csv"),
        ("Connecting stores and cameras", ConnectionDataCSV.ConnectionDataCSV, "example_store_camera_connections.adb.csv")
    ]

    for disp_message, data_class, in_file in loading_info:
        print(disp_message)
        in_csv_file = os.path.join(input_file_path, in_file)
        generator = data_class(in_csv_file)

        with open(error_file_path, 'w') as f, open(error_file_path + ".out", 'w') as g:
            with redirect_stderr(f), redirect_stdout(g):
                loader.ingest(
                    generator,
                    batchsize=batchsize,
                    numthreads=numthreads,
                    stats=stats)
        loader.print_stats()
        print("\n")
