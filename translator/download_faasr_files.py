#!/usr/bin/env python3
import argparse

from wfformat_reader import *
from workflow import *
from writer import *

from translator import *

"""
python3 download_faasr_files.py [faasr files json]

This program downloads the files specified in a faasr files json to an S3 bucket
"""


def main():
    # parse arguments
    parser = argparse.ArgumentParser(description="read faasr file json")
    parser.add_argument("faasr_files", help="faasr file information")
    parser.add_argument("--bucket_name", default=None, help="S3 bucket name")
    parser.add_argument("--endpoint", default=None, help="S3 endpoint URL")
    parser.add_argument("--access_key", default=None, help="S3 access key")
    parser.add_argument("--secret_key", default=None, help="S3 secret key")
    parser.add_argument(
        "--folder", default=None, help="Folder in S3 bucket to store files"
    )
    args = parser.parse_args()

    # Prompt for missing arguments
    bucket_name = args.bucket_name or input("Enter S3 bucket name: ")
    endpoint = args.endpoint or input("Enter S3 endpoint URL: ")
    access_key = args.access_key or input("Enter S3 access key: ")
    secret_key = args.secret_key or input("Enter S3 secret key: ")
    folder = args.folder or input("Enter folder in S3 bucket to store files: ")

    # download files to S3
    download_files_to_s3_from_json(
        args.faasr_files,
        bucket_name=bucket_name,
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        folder=folder,
    )


if __name__ == "__main__":
    main()
