#!/usr/bin/env python

import requests
import argparse
import os
from urllib.parse import urlparse

def get_filename_from_url(url):
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)

def download(url, output):
    if not output:
        output = get_filename_from_url(url)
    with open(output, 'wb') as f:
        response = requests.get(url)
        f.write(response.content)

def main():
    parser = argparse.ArgumentParser(description='Download a file from a URL.')
    parser.add_argument('url', type=str, help='The URL of the file to download')
    parser.add_argument('output', type=str, nargs='?', default='', help='The output file path (optional)')

    args = parser.parse_args()
    download(args.url, args.output)

if __name__ == "__main__":
    main()
