import boto3
import sys
import os

# Run this script by following this example
# python src/cc-downloader.py crawl-data/CC-MAIN-2024-18/segments/1606141753148.92/warc/CC-MAIN-20201206002041-20201206032041-00718.warc.gz
# Note that there is no bucket name; only prefixes
# File can be found in the tmp/ directory


def download_common_crawl_resource(filename, destination='raw/'):
    """ Downloads a common crawl resource and saves it to the destination directory"""
    s3 = boto3.client("s3")
    s3_resource = boto3.resource('s3')
    saved_file_name = f'./raw/{os.path.split(filename)[1]}'
    s3.download_file("commoncrawl", filename, saved_file_name)



if __name__ == '__main__':
    files_to_download = [
        "crawl-data/CC-MAIN-2024-22/segments/1715971057516.1/warc/CC-MAIN-20240518214304-20240519004304-00000.warc.gz"
    ]
    
    for file in files_to_download:
        download_common_crawl_resource(file)
