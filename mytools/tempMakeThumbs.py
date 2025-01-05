#!C:/Python37-32/python
# -*- coding: utf-8 -*-

import codecs
import os
from pathlib import Path
import psycopg
from psycopg.rows import namedtuple_row
from PIL import Image
import sys
from datetime import datetime

conn_info = "dbname=pixiv2a user=laravel password=secret host=localhost port=5432"
pxArtists_folder = Path("../PxArtists")
thumb_folder = pxArtists_folder / "thumbs"

import os

def find_folders_with_prefix(base_folder, prefix):
    try:
        # フォルダ内のすべてのエントリを調べる
        return [
            entry.name for entry in os.scandir(base_folder)
            if entry.is_dir() and entry.name.startswith(prefix)
        ]
    except FileNotFoundError:
        print(f"Error: Folder '{base_folder}' does not exist.")
        return []
    except PermissionError:
        print(f"Error: No permission to access '{base_folder}'.")
        return []

def find_files_with_prefix(base_folder, prefix):
    try:
        # フォルダ内のすべてのエントリを調べる
        return [
            entry.name for entry in os.scandir(base_folder)
            if entry.name.startswith(prefix)
        ]
    except FileNotFoundError:
        print(f"Error: File '{base_folder}/{prefix}' does not exist.")
        return []
    except PermissionError:
        print(f"Error: No permission to access '{base_folder}'.")
        return []

def createThumbWebP(originalImagePath, newImagePath):
    image = Image.open(originalImagePath)
    if os.path.exists(newImagePath):
        print(f'new image already exists at {newImagePath}')
        return

    image.thumbnail((320,320)) # convert to thumbnail
    os.makedirs(os.path.dirname(newImagePath), exist_ok=True)			
    image.save(newImagePath, "WEBP")

def iterImagesAndMakeThumbs(conn):

    with conn.cursor() as c:
        c.execute("select member_id, image_id, image_count from pixiv_master_image order by image_id")
        imageList = c.fetchall()
        print ("check_start")

    for row in imageList:
        base_folders = find_folders_with_prefix(pxArtists_folder, f"{row.member_id} - ")
        if (len(base_folders) == 1):
            for i in range(0, row.image_count):
                prefix = f"{row.image_id}_p{i}"
                filenames = find_files_with_prefix(pxArtists_folder / base_folders[0], prefix)
                if(len(filenames) == 1):
                    fullpath = pxArtists_folder / base_folders[0] / filenames[0]
                    fullpath_thumb = thumb_folder / str(row.member_id) / f"{row.image_id}_p{i}.webp" # idのみを使う 
                    createThumbWebP(fullpath, fullpath_thumb)
                elif len(filenames) == 0:
                    print(f"0000000 ERR: {prefix}")
                    continue
                else:
                    if 0 == i:
                        print(f"ERR: ??? {filenames}")
                        continue
                
        else:
            print(f"ERR: {base_folders} {row.member_id}")

if __name__ == "__main__":
    pass
    # dbconn = psycopg.connect(conn_info, row_factory=namedtuple_row)
    # dbconn.autocommit = True
    # iterImagesAndMakeThumbs(dbconn)

    # dbconn.close()
