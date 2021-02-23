from subprocess import Popen, PIPE
import boto3
from botocore.errorfactory import ClientError
from datetime import datetime, timedelta
import os
import glob
import time
import argparse
from queue import Queue
import threading
import shutil
transcode_done = threading.Event()
upload_done = threading.Event()

class Transcoder(threading.Thread):
    def __init__(
        self, input_queue, output_queue, ID,
        VIDEO_CODEC, OUTPUT_WIDTH, OUTPUT_HEIGHT,
        OUTPUT_BITRATE, VIDEO_DATE, REMOVE
    ):
        threading.Thread.__init__(self)
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.ID = ID
        self.VIDEO_CODEC = VIDEO_CODEC
        self.OUTPUT_WIDTH = OUTPUT_WIDTH
        self.OUTPUT_HEIGHT = OUTPUT_HEIGHT
        self.OUTPUT_BITRATE = OUTPUT_BITRATE
        self.VIDEO_DATE = VIDEO_DATE
        self.REMOVE = REMOVE
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
        self.working_dir = os.getcwd()
        self.archive_folder = os.path.join(
            self.working_dir, "archive", "original", self.VIDEO_DATE
        )
        if not os.path.isdir(self.archive_folder):
            os.makedirs(self.archive_folder)

    def postprocess(self, filepath):
        print(f"[{datetime.now()}] [Transcode{self.ID}] Postprocessing", flush=True)
        if self.REMOVE:
            print(f"[{datetime.now()}] [Transcode{self.ID}] Removing the file", flush=True)
            os.remove(filepath)
        else:
            print(f"[{datetime.now()}] [Transcode{self.ID}] Archiving the file", flush=True)
            filename = os.path.basename(filepath)
            shutil.move(
                os.path.join(self.working_dir, filepath),
                os.path.join(self.archive_folder, filename)
            )

    def run(self):
        while True:
            if self.input_queue.empty():
                if upload_done.is_set() and transcode_done.is_set():
                    print(f"[{datetime.now()}] [Transcode{self.ID}] Exit event is set. Exiting...", flush=True)
                    break
                elif upload_done.is_set() and not transcode_done.is_set():
                    print(f"[{datetime.now()}] [Transcode{self.ID}] Uploading done but transcoding is still working on another thread", flush=True)
                    time.sleep(10)
                    continue
                elif not upload_done.is_set() and transcode_done.is_set():
                    print(f"[{datetime.now()}] [Transcode{self.ID}] Uploading is still working on other threads", flush=True)
                    time.sleep(10)
                    continue
                elif not upload_done.is_set() and not transcode_done.is_set():
                    print(f"[{datetime.now()}] [Transcode{self.ID}] Transcoding and uploading still working on other threads", flush=True)
                    time.sleep(10)
                    continue
                else:
                    print(f"[{datetime.now()}] [Transcode{self.ID}] Waiting for upload to be done", flush=True)
                    time.sleep(10)
                    continue
            input_filepath = self.input_queue.get()
            print(f"[{datetime.now()}] [Transcode{self.ID}] Preparing to process {input_filepath}", flush=True)
            input_filename = os.path.basename(input_filepath)
            output_filepath = f"to_be_uploaded/resized_{input_filename}"
            command_string = [
                "gst-launch-1.0", "filesrc", f"location={input_filepath}", "!",
                "qtdemux", "!", self.codecs[self.VIDEO_CODEC]["parser"], "!",
                self.codecs[self.VIDEO_CODEC]["decoder"], "!", "nvvidconv", "!",
                f"video/x-raw(memory:NVMM), width=(int){self.OUTPUT_WIDTH}, height=(int){self.OUTPUT_HEIGHT}, format=(string)I420", "!",
                "omxh264enc", f"bitrate={self.OUTPUT_BITRATE}", "!",
                "qtmux", "!", "filesink", f"location={output_filepath}"
            ]
            if not os.path.isdir(os.path.split(output_filepath)[0]):
                os.makedirs(os.path.split(output_filepath)[0])
            
            process = Popen(command_string, stdout=PIPE, stderr=PIPE)
            print(f"[{datetime.now()}] [Transcode{self.ID}] Started transcoding", flush=True)
            result = process.communicate()
            returncode = process.returncode
            print(f"[{datetime.now()}] [Transcode{self.ID}] Finished transcoding with status {returncode}. Output:\n{result[0].decode()}", flush=True)
            if returncode <= 1:
                self.output_queue.put(output_filepath)
            self.postprocess(input_filepath)
            self.input_queue.task_done()
            if self.input_queue.empty() and not transcode_done.is_set():
                print(f"[{datetime.now()}] [Transcode{self.ID}] All the process done", flush=True)
                transcode_done.set()



        
class Uploader(threading.Thread):
    def __init__(
        self, queue, ID,
        S3_BUCKET, S3_FOLDER, VIDEO_DATE, REMOVE
    ):
        threading.Thread.__init__(self)
        self.queue = queue
        self.ID = ID
        self._init_s3()
        self.S3_BUCKET = S3_BUCKET
        self.S3_FOLDER = S3_FOLDER
        self.VIDEO_DATE = VIDEO_DATE
        self.REMOVE = REMOVE

        self.working_dir = os.getcwd()
        self.archive_folder = os.path.join(
            self.working_dir, "archive", "resized", self.VIDEO_DATE
        )
        if not os.path.isdir(self.archive_folder):
            os.makedirs(self.archive_folder)
    
    def _init_s3(self):
        self.s3_client = boto3.client('s3')

    def upload(self, filepath):
        print(f"[{datetime.now()}] [Upload{self.ID}] Preparing to upload {filepath}", flush=True)
        filename = os.path.basename(filepath)
        object_name = f'{self.S3_FOLDER}/{self.VIDEO_DATE}/{filename}'
        try:
            self.s3_client.head_object(Bucket=self.S3_BUCKET, Key=object_name)
            print(f"[{datetime.now()}] [Upload{self.ID}] File exists in S3 already. Skipping...", flush=True)
        except ClientError:
            print(f"[{datetime.now()}] [Upload{self.ID}] Uploading the file...", flush=True)
            response = self.s3_client.upload_file(
                filepath, self.S3_BUCKET, object_name
            )
            print(f"[{datetime.now()}] [Upload{self.ID}] Uploading finished", flush=True)
        self.postprocess(filepath)

    def postprocess(self, filepath):
        print(f"[{datetime.now()}] [Upload{self.ID}] Postprocessing", flush=True)
        if self.REMOVE:
            print(f"[{datetime.now()}] [Upload{self.ID}] Removing the file", flush=True)
            os.remove(filepath)
        else:
            print(f"[{datetime.now()}] [Upload{self.ID}] Archiving the file", flush=True)
            filename = os.path.basename(filepath)
            shutil.move(
                os.path.join(self.working_dir, filepath),
                os.path.join(self.archive_folder, filename)
            )
    
    def run(self):
        upload_count = 0
        while True:                
            if self.queue.empty():
                if transcode_done.is_set() and upload_done.is_set():
                    print(f"[{datetime.now()}] [Upload{self.ID}] Exit event is set. Exiting...", flush=True)
                    break
                elif not transcode_done.is_set() and upload_done.is_set():
                    print(f"[{datetime.now()}] [Upload{self.ID}] Uploading done but transcoding is still working on other threads", flush=True)
                    time.sleep(10)
                    continue
                elif transcode_done.is_set() and not upload_done.is_set():
                    print(f"[{datetime.now()}] [Upload{self.ID}] Transcoding done but uploading is still working on another thread", flush=True)
                    time.sleep(10)
                    continue
                elif not transcode_done.is_set() and not upload_done.is_set():
                    print(f"[{datetime.now()}] [Upload{self.ID}] Transcoding and uploading still working on other threads", flush=True)
                    time.sleep(10)
                    continue
            if upload_count > 80:
                print(f"[{datetime.now()}] [Upload{self.ID}] Renewing the S3 connection", flush=True)
                self._init_s3()
                upload_count = 0
            filepath = self.queue.get()
            self.upload(filepath)
            upload_count += 1
            self.queue.task_done()
            if self.queue.empty() and not upload_done.is_set():
                print(f"[{datetime.now()}] [Upload{self.ID}] All the processes done. Setting exit event", flush=True)
            
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--video_folder', type=str, default=None, help="A path where the transcoded videos will be stored")
    parser.add_argument('--video_date', type=str, default=None, help="Seaching date in the format %Y-%m-%d")
    parser.add_argument('--remove_original', type=bool, nargs='?', const=True, default=False, help="Removing the file afterward")
    parser.add_argument('--remove_resized', type=bool, nargs='?', const=True, default=False, help="Removing the file afterward")
    args = parser.parse_args()

    VIDEO_DATE      = args.video_date if args.video_date else (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    VIDEO_CODEC     = "h265" #"mpeg4"
    OUTPUT_WIDTH    = 320 #480 
    OUTPUT_HEIGHT   = 240 #270
    OUTPUT_BITRATE  = 30000
    VIDEO_FOLDER    = args.video_folder
    REMOVE_ORG      = args.remove_original
    REMOVE_RES      = args.remove_resized
    S3_BUCKET       = "asilla-abr"
    S3_FOLDER       = "2020-11_Fukagawa_Gatharia_PoC" #"2020-12_Machida_Zelvia_PoC"
    
    transcode_workers = 2
    upload_workers = 2
    transcode_queue = Queue(maxsize=1000)
    upload_queue = Queue(maxsize=1000)
    glob_search_path = os.path.join(VIDEO_FOLDER, f"*cam0*_record_{VIDEO_DATE}*.mp4")
    filepath_list = glob.glob(glob_search_path)

    
    for i in range(1, transcode_workers+1):
        print(f'[{datetime.now()}] [Main] Adding transcoder thread {i} for queue processing')
        transcode_thread = Transcoder(
            transcode_queue, upload_queue, i,
            VIDEO_CODEC, OUTPUT_WIDTH, OUTPUT_HEIGHT,
            OUTPUT_BITRATE, VIDEO_DATE, REMOVE_ORG
        )
        transcode_thread.daemon = True
        transcode_thread.start()
    
    for filepath in filepath_list:
        transcode_queue.put(filepath)
    
    time.sleep(10)

    for i in range(1, upload_workers+1):
        print(f'[{datetime.now()}] [Main] Adding uploader thread {i} for queue processing')
        upload_thread = Uploader(
            upload_queue, i,
            S3_BUCKET, S3_FOLDER, VIDEO_DATE, REMOVE_RES
        )
        upload_thread.daemon = True
        upload_thread.start()

    print(f'[{datetime.now()}] [Main] Waiting for queue to be completed')
    transcode_queue.join()
    upload_queue.join()
    print(f'[{datetime.now()}] [Main] Done')