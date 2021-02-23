from subprocess import Popen, PIPE
import boto3
from botocore.errorfactory import ClientError
from datetime import datetime
import os
import glob
import shutil
import argparse
import time

class MediaFactory:
    def __init__(
        self, RUN_TRANSCODE, RUN_UPLOAD,
        REMOVE_FILE, VIDEO_DATE, VIDEO_CODEC,
        OUTPUT_WIDTH, OUTPUT_HEIGHT, OUTPUT_BITRATE,
        S3_BUCKET, S3_FOLDER
    ):
        self.RUN_TRANSCODE = RUN_TRANSCODE
        self.RUN_UPLOAD = RUN_UPLOAD
        self.REMOVE_FILE = REMOVE_FILE

        self.VIDEO_DATE = VIDEO_DATE
        self.VIDEO_CODEC = VIDEO_CODEC
        self.OUTPUT_WIDTH = OUTPUT_WIDTH
        self.OUTPUT_HEIGHT = OUTPUT_HEIGHT
        self.OUTPUT_BITRATE = OUTPUT_BITRATE

        self.S3_BUCKET = S3_BUCKET
        self.S3_FOLDER = S3_FOLDER

        self.s3_client = boto3.client('s3')
        self.INPUT_FILEPATH = None
        self.OUTPUT_FILEPATH = None

        self.codecs = {
            "mpeg4": {
                "parser": "mpeg4videoparse",
                "decoder": "omxmpeg4videodec"
            },
            "h264": {
                "parser": "h264parse",
                "decoder": "omxh264dec"
            },
            "h265": {
                "parser": "h265parse",
                "decoder": "omxh265dec"
            }
        }

        self.upload_count = 0

    def transcode_gstreamer(self):
        input_filename = os.path.basename(self.INPUT_FILEPATH)
        self.OUTPUT_FILEPATH = f"to_be_uploaded/resized_{input_filename}"
        command_string = [
            "gst-launch-1.0", "filesrc", f"location={self.INPUT_FILEPATH}", "!",
            "qtdemux", "!", self.codecs[self.VIDEO_CODEC]["parser"], "!",
            self.codecs[self.VIDEO_CODEC]["decoder"], "!", "nvvidconv", "!",
            f"video/x-raw(memory:NVMM), width=(int){OUTPUT_WIDTH}, height=(int){OUTPUT_HEIGHT}, format=(string)I420", "!",
            "omxh264enc", f"bitrate={self.OUTPUT_BITRATE}", "!",
            "qtmux", "!", "filesink", f"location={self.OUTPUT_FILEPATH}"
        ]
        if not os.path.isdir(os.path.split(self.OUTPUT_FILEPATH)[0]):
            os.makedirs(os.path.split(self.OUTPUT_FILEPATH)[0])
        
        process = Popen(command_string, stdout=PIPE, stderr=PIPE)
        print(f"[{datetime.now()}] Started transcoding", flush=True)
        result = process.communicate()
        returncode = process.returncode
        print(f"[{datetime.now()}] Finished transcoding with status {returncode}. Output:\n{result[0].decode()}", flush=True)
        return returncode
    
    def upload_to_s3(self):
        filename = os.path.basename(self.OUTPUT_FILEPATH)
        object_name = f'{self.S3_FOLDER}/{self.VIDEO_DATE}/{filename}'
        if self.upload_count > 200:
            self.s3_client = boto3.client('s3')
            time.sleep(1)
            self.upload_count = 0
        try:
            self.s3_client.head_object(Bucket=self.S3_BUCKET, Key=object_name)
            print(f"[{datetime.now()}] File exists in S3 already. Skipping...", flush=True)
        except ClientError:
            print(f"[{datetime.now()}] Uploading the file...", flush=True)
            response = self.s3_client.upload_file(
                self.OUTPUT_FILEPATH, self.S3_BUCKET, object_name
            )
            self.upload_count += 1
    
    def move_file_to_archive(self, filetype):
        print(f"[{datetime.now()}] Moving the file to archive", flush=True)
        current_dir = os.getcwd()
        types = {
            "input": {
                "filepath": self.INPUT_FILEPATH,
                "filename": os.path.basename(self.INPUT_FILEPATH) if self.INPUT_FILEPATH else None,
                "folder": "original"
            },
            "output": {
                "filepath": self.OUTPUT_FILEPATH,
                "filename": os.path.basename(self.OUTPUT_FILEPATH) if self.OUTPUT_FILEPATH else None,
                "folder": "resized"
            }
        }
        target_folder = os.path.join(
            current_dir, "archive", types[filetype]["folder"],self.VIDEO_DATE
        )
        if not os.path.isdir(target_folder):
            os.makedirs(target_folder)
        
        shutil.move(
            os.path.join(current_dir, types[filetype]["filepath"]),
            os.path.join(target_folder, types[filetype]["filename"])
        )

    def run(self):
        if self.RUN_TRANSCODE:
            print(f"[{datetime.now()}] Processing {self.INPUT_FILEPATH}", flush=True)
            returncode = self.transcode_gstreamer()

            if returncode <= 1 and not self.REMOVE_FILE:
                self.move_file_to_archive('input')

            elif returncode <= 1 and self.REMOVE_FILE:
                os.remove(self.INPUT_FILEPATH)
                print(f"[{datetime.now()}] Deleted the file", flush=True)

            else:
                print(f"[{datetime.now()}] The Gstreamer process failed", flush=True)
                self.RUN_UPLOAD = False if self.RUN_UPLOAD else None
        
        if self.RUN_UPLOAD:
            print(f"[{datetime.now()}] Preparing to upload {self.OUTPUT_FILEPATH}", flush=True)
            self.upload_to_s3()
            self.move_file_to_archive('output')
        
        print(f"[{datetime.now()}] DONE", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--transcode', type=bool, nargs='?', const=True, default=False, help="Run the transcoding")
    parser.add_argument('--upload', type=bool, nargs='?', const=True, default=False, help="Uploading the videos to S3")
    parser.add_argument('--remove', type=bool, nargs='?', const=True, default=False, help="Removing the file afterward")
    parser.add_argument('--video_folder', type=str, default=None, help="A path where the transcoded videos will be stored")
    parser.add_argument('--video_date', type=str, default=None, help="Seaching date in the format %Y-%m-%d")
    args = parser.parse_args()

    RUN_TRANSCODE   = args.transcode
    RUN_UPLOAD      = args.upload
    REMOVE_FILE     = args.remove
    VIDEO_DATE      = args.video_date if args.video_date else (datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    VIDEO_CODEC     = "mpeg4" #"h265"
    OUTPUT_WIDTH    = 480 #320 
    OUTPUT_HEIGHT   = 270 #240
    OUTPUT_BITRATE  = 50000
    VIDEO_FOLDER    = args.video_folder
    S3_BUCKET       = "asilla-abr"
    S3_FOLDER       = "2020-12_Machida_Zelvia_PoC" #"2020-11_Fukagawa_Gatharia_PoC"
    
    mediafactory = MediaFactory(
        RUN_TRANSCODE=RUN_TRANSCODE,
        RUN_UPLOAD=RUN_UPLOAD,
        REMOVE_FILE=REMOVE_FILE,
        VIDEO_DATE=VIDEO_DATE,
        VIDEO_CODEC=VIDEO_CODEC,
        OUTPUT_WIDTH=OUTPUT_WIDTH,
        OUTPUT_HEIGHT=OUTPUT_HEIGHT,
        OUTPUT_BITRATE=OUTPUT_BITRATE,
        S3_BUCKET=S3_BUCKET,
        S3_FOLDER=S3_FOLDER
    )
    glob_search_path = os.path.join(VIDEO_FOLDER, f"*cam0*_record_{VIDEO_DATE}*.mp4")
    video_list = glob.glob(glob_search_path)

    for filepath in video_list:
        if RUN_TRANSCODE and not RUN_UPLOAD:
            mediafactory.INPUT_FILEPATH = filepath
        
        elif not RUN_TRANSCODE and RUN_UPLOAD:
            mediafactory.OUTPUT_FILEPATH = filepath

        else:
            mediafactory.INPUT_FILEPATH = filepath
        mediafactory.run()
