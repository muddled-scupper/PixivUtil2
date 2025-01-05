#!C:/Python37-32/python
# -*- coding: utf-8 -*-

import codecs
import os
import re
import psycopg
from psycopg.rows import namedtuple_row
import sys
from datetime import datetime

#conn_info = "dbname=pixiv2a user=laravel password=secret host=postgres port=5432"
conn_info = "dbname=pixiv2a user=laravel password=secret host=localhost port=5432"
base_folder = "../PxArtists"

# /workdir/PxArtists/nnnnn - Artist/nnnnn_p55_master1200 - comment.jpg
# /workdir/PxArtists/{数値} - {ユーザー名}/{数値}_p{最終ページ番号}_master{任意の文字列}.jpg
def extract_last_page_number(file_path):
    pattern = r"/\d+_p(\d+)_master"
    match = re.search(pattern, file_path)
    
    if match:
        return 1 + int(match.group(1))
    return 0

def registerPagesToDbB(conn, folder_names):
    with conn.cursor() as c:
        # c.execute("select member_id from pixiv_master_member order by member_id")
        # memberList = c.fetchall()
        c.execute("select image_id, save_name from pixiv_master_image order by image_id")
        imageList = c.fetchall()
        print ("check_start")

    for row in imageList:
        pnum = extract_last_page_number(row.save_name)
        # print(f"{pnum} <- {row.save_name}")
        c = conn.cursor()
        with conn.transaction():
            c.execute("update pixiv_master_image set image_count = %s where image_id = %s", (pnum, row.image_id))
            print(f"{pnum} <- {row.image_id}")

if __name__ == "__main__":
    pass
    # dbconn = psycopg.connect(conn_info, row_factory=namedtuple_row)
    # dbconn.autocommit = True
    # folder_names = [entry.name for entry in os.scandir(base_folder) if entry.is_dir()]
    # registerPagesToDbB(dbconn, folder_names)

    # dbconn.close()
