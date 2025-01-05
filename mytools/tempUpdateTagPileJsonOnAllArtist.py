#!C:/Python37-32/python
# -*- coding: utf-8 -*-

import codecs
import os
import re
import psycopg
from psycopg.rows import namedtuple_row
from psycopg.types.json import Json
import sys
from datetime import datetime

#conn_info = "dbname=pixiv2a user=laravel password=secret host=postgres port=5432"
conn_info = "dbname=pixiv2a user=laravel password=secret host=localhost port=5432"
base_folder = "../PxArtists"

def extract_last_page_number(file_path):
    pattern = r"/\d+_p(\d+)_master"
    match = re.search(pattern, file_path)
    
    if match:
        return 1 + int(match.group(1))
    return 0

def updateTagPileOnMembers(conn):
    with conn.cursor() as c:
        query = '''select pmm.member_id, pmm.name, pitt.tag_id, count(pitt.tag_id) as count from pixiv_image_to_tag pitt 
            left join pixiv_master_image pmi on pitt.image_id = pmi.image_id 
            left join pixiv_master_member pmm on pmi.member_id = pmm.member_id 
            group by pitt.tag_id, pmm.member_id, pmm.name
            order by pmm.member_id asc, count desc;'''
        c.execute(query)
        qResult = c.fetchall()
        memberData = {}
        print ("check_start")

    for row in qResult:
        # [[tag_id(=name), count], .....]
        if row.member_id not in memberData.keys():
            jsonData = []
        else:
            jsonData = memberData[row.member_id]
        jsonData.append([row.tag_id, row.count])
        memberData[row.member_id] = jsonData
    
    for key in memberData:
        c = conn.cursor()
        with conn.transaction():
            # see https://www.psycopg.org/psycopg3/docs/basic/adapt.html#adapt-json
            c.execute("update pixiv_master_member set tag_pile = %s where member_id = %s", (Json(memberData[key]), key))
            print(c._query)

if __name__ == "__main__":
    dbconn = psycopg.connect(conn_info, row_factory=namedtuple_row)
    dbconn.autocommit = True
    updateTagPileOnMembers(dbconn)

    dbconn.close()
