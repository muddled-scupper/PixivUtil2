#!C:/Python37-32/python
# -*- coding: utf-8 -*-

import codecs
import os
import re
import psycopg #sqlite3
import sys
from datetime import datetime

# import colorama
from colorama import Back, Fore, Style

import PixivHelper
from PixivListItem import PixivListItem

from PixivException import PixivException

script_path = PixivHelper.module_path()


class PixivDBManager(object):
    """Pixiv Database Manager"""

    def __init__(self, conn_info, timeout=5 * 60):
        self.conn = psycopg.connect(conn_info)
        self.conn.autocommit = True

    def close(self):
        self.conn.close()

##########################################
# I. Create/Drop Database                #
##########################################
    def createDatabase(self):
        print('Creating database...', end=' ')

        try:
            c = self.conn.cursor()
            with self.conn.transaction():

                c.execute('''CREATE TABLE IF NOT EXISTS pixiv_master_member (
                                member_id bigint PRIMARY KEY,
                                name TEXT,
                                save_folder TEXT,
                                created_date timestamptz,
                                last_update_date timestamptz,
                                last_image bigint,
                                is_deleted bigint DEFAULT 0,
                                member_token TEXT
                                )''')
        
                c.execute('''CREATE TABLE IF NOT EXISTS pixiv_master_image (
                                image_id bigint PRIMARY KEY,
                                member_id bigint,
                                title TEXT,
                                save_name TEXT,
                                created_date timestamptz,
                                last_update_date timestamptz,
                                is_manga TEXT, 
                                caption TEXT
                                )''')
        
                c.execute('''CREATE TABLE IF NOT EXISTS pixiv_manga_image (
                                image_id bigint,
                                page bigint,
                                save_name TEXT,
                                created_date timestamptz,
                                last_update_date timestamptz,
                                PRIMARY KEY (image_id, page)
                                )''')
        
                # Pixiv Tags
                c.execute('''CREATE TABLE IF NOT EXISTS pixiv_master_tag (
                                tag_id VARCHAR(255) PRIMARY KEY,
                                created_date timestamptz,
                                last_update_date timestamptz
                                )''')
        
                c.execute('''CREATE TABLE IF NOT EXISTS pixiv_tag_translation (
                                tag_id VARCHAR(255) REFERENCES pixiv_master_tag(tag_id),
                                translation_type VARCHAR(255),
                                translation VARCHAR(255),
                                created_date timestamptz,
                                last_update_date timestamptz,
                                PRIMARY KEY (tag_id, translation_type)
                                )''')
        
                c.execute('''CREATE TABLE IF NOT EXISTS pixiv_image_to_tag (
                                image_id bigint REFERENCES pixiv_master_image(image_id),
                                tag_id VARCHAR(255) REFERENCES pixiv_master_tag(tag_id),
                                created_date timestamptz,
                                last_update_date timestamptz,
                                PRIMARY KEY (image_id, tag_id)
                                )''')

            # FANBOX

            # Sketch

            # Novel
            # self.create_update_novel_table(c)

            print('done.')
        # except BaseException:
        #     print('Error at createDatabase():', str(sys.exc_info()))
        #     print('failed.')
        except Exception as e:
            print(f"An error occurred: {e}")
            raise
        finally:
            c.close()

    def dropDatabase(self):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():

                c.execute('''DROP TABLE IF EXISTS pixiv_image_to_tag''')
                c.execute('''DROP TABLE IF EXISTS pixiv_tag_translation''')
                c.execute('''DROP TABLE IF EXISTS pixiv_master_tag''')
        
                c.execute('''DROP TABLE IF EXISTS pixiv_master_member''')
        
                c.execute('''DROP TABLE IF EXISTS pixiv_master_image''')
        
                c.execute('''DROP TABLE IF EXISTS pixiv_manga_image''')
        
                c.execute('''DROP TABLE IF EXISTS fanbox_master_post''')
                c.execute('''DROP TABLE IF EXISTS fanbox_post_image''')
        
                c.execute('''DROP TABLE IF EXISTS sketch_master_post''')
                c.execute('''DROP TABLE IF EXISTS sketch_post_image''')
    
        except BaseException:
            print('Error at dropDatabase():', str(sys.exc_info()))
            print('failed.')
            raise
        finally:
            c.close()
        print('done.')

    def compactDatabase(self):
        pass

##########################################
# II. Export/Import DB                  #
##########################################
    def importList(self, listTxt):
        print('Importing list...', end=' ')
        print('Found', len(listTxt), 'items', end=' ')
        try:
            c = self.conn.cursor()

            for item in listTxt:
                with self.conn.transaction():
                    c.execute('''INSERT INTO pixiv_master_member VALUES(%s, %s, %s, CURRENT_TIMESTAMP, '1-1-1', -1, 0, '')''',
                            (item.memberId, str(item.memberId), r'N\A'))
                    c.execute('''UPDATE pixiv_master_member
                                SET save_folder = %s
                                WHERE member_id = %s ''',
                            (item.path, item.memberId))
        except BaseException:
            print('Error at importList():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()
        print('done.')
        return 0

    def exportImageTable(self, name):
        print(f'Exporting {name} table ...', end=' ')
        im_list = list()
        if name == "Pixiv":
            table = "pixiv_master_image"
            key = "image_id"
        elif name == "Fanbox":
            table = "fanbox_master_post"
            key = "post_id"
        elif name == "Sketch":
            table = "sketch_master_post"
            key = "post_id"
        else:
            raise
        try:
            c = self.conn.cursor()
            c.execute(f''' SELECT COUNT(*) FROM {table}''')
            result = c.fetchall()
            if result[0][0] > 10000:
                print('Row count is more than 10000 (actual row count:',
                      str(result[0][0]), ')')
                print('It may take a while to retrieve the data.')
                arg = input('Continue [y/n, default is yes]').rstrip("\r") or 'y'
                answer = arg.lower()
                if answer not in ('y', 'n', 'o'):
                    PixivHelper.print_and_log("error", f"Invalid args for TODO: {arg}, valid values are [y/n/o].")
                    return
                if answer == 'y':
                    c = self.conn.cursor()
                    c.execute(f'''SELECT {key}
                                FROM {table}
                                ORDER BY member_id''')
                    for row in c:
                        im_list.append(row[0])
            else:
                c.execute(f'''SELECT {key}
                            FROM {table}
                            ORDER BY member_id''')
                for row in c:
                    im_list.append(row[0])
            c.close()
            print('done.')
            return im_list
        except BaseException:
            print('Error at exportImageTable():', str(sys.exc_info()))
            print('failed')
            raise

    def exportList(self, filename, include_artist_token=True):
        print('Exporting list...', end=' ')
        try:
            c = self.conn.cursor()
            c.execute('''SELECT member_id, save_folder, name, member_token
                         FROM pixiv_master_member
                         WHERE is_deleted = 0
                         ORDER BY member_id''')
            if not filename.endswith(".txt"):
                filename = filename + '.txt'

            writer = codecs.open(filename, 'wb', encoding='utf-8')
            writer.write('###Export date: {0} ###\r\n'.format(datetime.today()))
            for row in c:
                if include_artist_token:
                    data = row[2]
                    token = row[3]
                    writer.write(f"# name: {data},token: {token}")
                    writer.write("\r\n")
                writer.write(str(row[0]))
                if len(row[1]) > 0:
                    writer.write(' ' + str(row[1]))
                writer.write('\r\n')
            writer.write('###END-OF-FILE###')
        except BaseException:
            print('Error at exportList():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            if writer is not None:
                writer.close()
            c.close()
        print('done.')

    def exportDetailedList(self, filename):
        print('Exporting detailed list...', end=' ')
        try:
            c = self.conn.cursor()
            c.execute('''SELECT * FROM pixiv_master_member
                            WHERE is_deleted = 0
                            ORDER BY member_id''')
            filename = filename + '.csv'
            writer = codecs.open(filename, 'wb', encoding='utf-8')
            writer.write('member_id,name,save_folder,created_date,last_update_date,last_image,is_deleted,member_token\r\n')
            for row in c:
                for string in row:
                    # Unicode write!!
                    data = string
                    writer.write(data)
                    writer.write(',')
                writer.write('\r\n')
            writer.write('###END-OF-FILE###')
            writer.close()
        except BaseException:
            print('Error at exportDetailedList(): ' + str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()
        print('done.')

    def exportFanboxPostList(self, filename, sep=","):
        print('Exporting FANBOX post list...', end=' ')
        try:
            c = self.conn.cursor()
            c.execute('''SELECT * FROM fanbox_master_post
                            ORDER BY member_id, post_id''')
            filename = filename + '.csv'
            writer = codecs.open(filename, 'wb', encoding='utf-8')
            columns = ['member_id', 'post_id', 'title', 'fee_required', 'published_date', 'update_date', 'post_type', 'last_update_date']
            writer.write(sep.join(columns))
            writer.write('\r\n')
            for row in c:
                writer.write(sep.join([str(x) for x in row]))
                writer.write('\r\n')
            writer.write('###END-OF-FILE###')
            writer.close()
        except BaseException:
            print('Error at exportFanboxPostList(): ' + str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()
        print('done.')

##########################################
# III. Print DB                          #
##########################################
    def printMemberList(self, isDeleted=False):
        print('Printing member list:')
        try:
            c = self.conn.cursor()
            c.execute('''SELECT * FROM pixiv_master_member
                         WHERE is_deleted = %s ORDER BY member_id''', (int(isDeleted), ))
            print('%10s %25s %25s %20s %20s %10s %s %s' % ('member_id',
                                                        'name',
                                                        'save_folder',
                                                        'created_date',
                                                        'last_update_date',
                                                        'last_image',
                                                        'is_deleted',
                                                        'member_token'))
            i = 0
            for row in c:
                print('%10d %#25s %#25s %20s %20s %10d %5s' %
                      (row[0], row[1].strip(), row[2], row[3], row[4], row[5], row[6]))
                i = i + 1
                if i == 79:
                    select = input('Continue [y/n, default is yes]? ').rstrip("\r")
                    if select == 'n':
                        break
                    else:
                        print(
                            'member_id\tname\tsave_folder\tcreated_date\tlast_update_date\tlast_image')
                        i = 0
        except BaseException:
            print('Error at printMemberList():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()
        print('done.')

    def printImageList(self):
        print('Printing image list:')
        try:
            c = self.conn.cursor()
            c.execute(''' SELECT COUNT(*) FROM pixiv_master_image''')
            result = c.fetchall()
            if result[0][0] > 10000:
                print('Row count is more than 10000 (actual row count:',
                      str(result[0][0]), ')')
                print('It may take a while to retrieve the data.')
                answer = input('Continue [y/n, default is no]').rstrip("\r")
                if answer == 'y':
                    c.execute('''SELECT * FROM pixiv_master_image
                                    ORDER BY member_id''')
                    print('')
                    for row in c:
                        for string in row:
                            print('   ', end=' ')
                            print(string)
                        print('')
                else:
                    return
            # Yavos: it seems you forgot something ;P
            else:
                c.execute(
                    '''SELECT * FROM pixiv_master_image ORDER BY member_id''')
                print('')
                for row in c:
                    for string in row:
                        print('   ', end=' ')
                        print(string)
                    print('')
            # Yavos: end of change
        except BaseException:
            print('Error at printImageList():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()
        print('done.')

##########################################
# IV. CRUD Member Table                  #
##########################################
    def insertNewMember(self, member_id=0, member_token=None):
        try:
            c = self.conn.cursor()
            if member_id == 0:
                while True:
                    temp = input('Member ID: ').rstrip("\r")
                    try:
                        member_id = int(temp)
                    except BaseException:
                        pass
                    if member_id > 0:
                        break

            c.execute("select member_id from pixiv_master_member where member_id = %s", (member_id,))
            if None == c.fetchone():
                with self.conn.transaction():
                    c.execute('''INSERT INTO pixiv_master_member VALUES(%s, %s, %s, CURRENT_TIMESTAMP, '1-1-1', -1, 0, %s)''',
                            (member_id, str(member_id), r'N\A', member_token))
            
        except BaseException:
            print('Error at insertNewMember():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectAllMember(self, isDeleted=False):
        members = list()
        try:
            c = self.conn.cursor()
            c.execute('''SELECT member_id, save_folder FROM pixiv_master_member WHERE is_deleted = %s ORDER BY member_id''',
                      (int(isDeleted), ))
            result = c.fetchall()

            for row in result:
                item = PixivListItem(row[0], row[1])
                members.append(item)

        except BaseException:
            print('Error at selectAllMember():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

        return members

    def selectMembersByLastDownloadDate(self, difference):
        members = list()
        try:
            c = self.conn.cursor()
            try:
                int_diff = int(difference)
            except ValueError:
                int_diff = 7

            c.execute('''SELECT member_id, save_folder,  (julianday(Date('now')) - julianday(last_update_date)) as diff
                         FROM pixiv_master_member
                         WHERE is_deleted <> 1 AND ( last_update_date == '1-1-1' OR diff > %s ) ORDER BY member_id''', (int_diff, ))
            result = c.fetchall()
            for row in result:
                item = PixivListItem(row[0], row[1])
                members.append(item)

        except BaseException:
            print('Error at selectMembersByLastDownloadDate():',
                  str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

        return members

    def selectMemberByMemberId(self, member_id):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT * FROM pixiv_master_member WHERE member_id = %s ''', (member_id, ))
            return c.fetchone()
        except BaseException:
            print('Error at selectMemberByMemberId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectMemberByMemberId2(self, member_id):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT member_id, save_folder FROM pixiv_master_member WHERE member_id = %s ''', (member_id, ))
            row = c.fetchone()
            if row is not None:
                return PixivListItem(row[0], row[1])
            else:
                return PixivListItem(int(member_id), '')
        except BaseException:
            print('Error at selectMemberByMemberId2():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def printMembersByLastDownloadDate(self, difference):
        rows = self.selectMembersByLastDownloadDate(difference)

        for row in rows:
            for string in row:
                print('   ', end=' ')
                print(string)
            print('\n')

    def updateMemberName(self, memberId, memberName, member_token):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''UPDATE pixiv_master_member
                                SET name = %s, member_token = %s
                                WHERE member_id = %s
                                ''', (memberName, member_token, memberId))
        except BaseException:
            print('Error at updateMemberName():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def updateSaveFolder(self, memberId, saveFolder):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''UPDATE pixiv_master_member
                                SET save_folder = %s
                                WHERE member_id = %s
                                ''', (saveFolder, memberId))
        except BaseException:
            print('Error at updateSaveFolder():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def updateLastDownloadedImage(self, memberId, imageId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''UPDATE pixiv_master_member
                            SET last_image = %s, last_update_date = CURRENT_TIMESTAMP
                            WHERE member_id = %s''', (imageId, memberId))
        except BaseException:
            print('Error at updateLastDownloadedImage:', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def updateLastDownloadDate(self, memberId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute("""UPDATE pixiv_master_member
                            SET last_update_date = CURRENT_TIMESTAMP
                            WHERE member_id = %s""", (memberId,))
        except BaseException:
            print('Error at updateLastDownloadDate():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteMemberByMemberId(self, memberId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''DELETE FROM pixiv_master_member
                        WHERE member_id = %s''', (memberId, ))
        except BaseException:
            print('Error at deleteMemberByMemberId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteMembersByList(self):
        list_name = input("Members filename = ").rstrip("\r")

        if len(list_name) == 0:
            list_name = "members.txt"

        listTxt = PixivListItem.parseList(list_name)

        print('Reading list...', end=' ')
        print('Found', len(listTxt), 'items', end=' ')

        try:
            c = self.conn.cursor()
            for item in listTxt:
                with self.conn.transaction():
                    c.execute('''DELETE FROM pixiv_manga_image
                            WHERE EXISTS (SELECT * FROM pixiv_master_image WHERE member_id = %s)''', (item.memberId, ))
                    c.execute('''DELETE FROM pixiv_master_image
                            WHERE member_id = %s''', (item.memberId, ))
                    c.execute('''DELETE FROM pixiv_master_member
                            WHERE member_id = %s''', (item.memberId, ))
        except BaseException:
            print('Error at deleteMembersByList():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def keepMembersByList(self):
        def parseMembersList(filename):
            memberList = list()
            reader = PixivHelper.open_text_file(filename)
            line_no = 1

            try:
                for line in reader:
                    original_line = line
                    if line.startswith('#') or len(line) < 1:
                        continue
                    if len(line.strip()) == 0:
                        continue
                    
                    line = line.strip()
                    memberList.append(line)
                    line_no = line_no + 1
                    original_line = ""

            except UnicodeDecodeError:
                PixivHelper.get_logger().exception("PixivDBManager.parseMembersList(): Invalid value when parsing list")
                PixivHelper.print_and_log('error', 'Invalid value: {0} at line {1}, try to save the list.txt in UTF-8.'.format(
                                        original_line, line_no))
            except BaseException:
                PixivHelper.get_logger().exception("PixivDBManager.parseMembersList(): Invalid value when parsing list")
                PixivHelper.print_and_log('error', 'Invalid value: {0} at line {1}'.format(original_line, line_no))
            finally:
                reader.close()
                return memberList
        
        list_name = input("Members filename = ").rstrip("\r")

        if len(list_name) == 0:
            list_name = "members.txt"

        if os.path.exists(list_name):
            listTxt = parseMembersList(list_name)
        else:
            msg = f"List file not found: {list_name}"
            raise PixivException("File doesn't exists or no permission to read: " + list_name,
                                    errorCode=PixivException.FILE_NOT_EXISTS_OR_NO_WRITE_PERMISSION)

        print('Reading list...', end=' ')
        print('Found', len(listTxt), 'items', end=' ')

        try:
            c = self.conn.cursor()
            c.execute('''SELECT * FROM pixiv_master_member''')
            result = c.fetchall()
            for row in result:
                if str(row[0]) not in listTxt:
                    with self.conn.transaction():
                        c.execute('''DELETE FROM pixiv_manga_image
                            WHERE EXISTS (SELECT * FROM pixiv_master_image WHERE member_id = %s)''', (row[0], ))
                        c.execute('''DELETE FROM pixiv_master_image
                            WHERE member_id = %s''', (row[0], ))
                        c.execute('''DELETE FROM pixiv_master_member
                            WHERE member_id = %s''', (row[0], ))
        except BaseException:
            print('Error at keepMembersByList():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteCascadeMemberByMemberId(self, memberId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''DELETE FROM pixiv_manga_image
                        WHERE EXISTS (SELECT * FROM pixiv_master_image WHERE member_id = %s)''', (memberId, ))
                c.execute('''DELETE FROM pixiv_master_image
                        WHERE member_id = %s''', (memberId, ))
                c.execute('''DELETE FROM pixiv_master_member
                        WHERE member_id = %s''', (memberId, ))
        except BaseException:
            print('Error at deleteCascadeMemberByMemberId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def setIsDeletedFlagForMemberId(self, memberId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''UPDATE pixiv_master_member
                            SET is_deleted = 1, last_update_date = CURRENT_TIMESTAMP
                            WHERE member_id = %s''', (memberId,))
        except BaseException:
            print('Error at setIsDeletedFlagForMemberId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

##########################################
# V. CRUD Image Table                    #
##########################################

    def insertTag(self, tag_id):
        try:
            c = self.conn.cursor()
            c.execute("select tag_id from pixiv_master_tag where tag_id = %s", (tag_id,))
            if None == c.fetchone():
                with self.conn.transaction():
                    c.execute('''INSERT INTO pixiv_master_tag VALUES (%s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''',
                            (tag_id,))
        except BaseException:
            print('Error at insertTag():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def insertImageToTag(self, image_id, tag_id):
        try:
            c = self.conn.cursor()
            image_id = int(image_id)

            with self.conn.transaction():
                c.execute('''INSERT INTO pixiv_image_to_tag(image_id, tag_id, created_date, last_update_date) 
                        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT(image_id, tag_id) DO UPDATE SET last_update_date = CURRENT_TIMESTAMP''',
                        (image_id, tag_id))
        except BaseException:
            print('Error at insertImageToTag():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def insertTagTranslation(self, tag_id, translation_type, translation):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''INSERT INTO pixiv_tag_translation(tag_id, translation_type, translation, created_date, last_update_date) 
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT(tag_id, translation_type) DO UPDATE SET 
                    translation = excluded.translation,
                    last_update_date = CURRENT_TIMESTAMP''',
                    (tag_id, translation_type, translation))
        except BaseException:
            print('Error at insertImageToTag():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectImagesByTagId(self, tag_id):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT pixiv_master_image.* 
                FROM pixiv_master_image
                JOIN pixiv_image_to_tag ON pixiv_master_image.image_id = pixiv_image_to_tag.image_id
                WHERE pixiv_image_to_tag.tag_id = %s
                ''', (tag_id,))
            return c.fetchall()
        except BaseException:
            print('Error at selectImagesByTagId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectTagsByImageId(self, image_id):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT pixiv_master_tag.* 
                FROM pixiv_master_tag
                JOIN pixiv_image_to_tag ON pixiv_image_to_tag.tag_id = pixiv_master_tag.tag_id
                WHERE pixiv_image_to_tag.image_id = %s
                ''', (image_id,))
            return c.fetchall()
        except BaseException:
            print('Error at selectTagsByImageId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteImagesByTag(self, tag_id):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''DELETE FROM pixiv_master_image
                        WHERE image_id IN (SELECT image_id FROM pixiv_image_to_tag WHERE tag_id = %s)''',
                        (tag_id, ))
                c.execute('''DELETE FROM pixiv_manga_image
                        WHERE image_id IN (SELECT image_id FROM pixiv_image_to_tag WHERE tag_id = %s)''',
                        (tag_id, ))
                c.execute('''DELETE FROM pixiv_image_to_tag WHERE tag_id = %s''', (tag_id, ))
        except BaseException:
            print('Error at deleteImage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def insertImage(self, member_id, image_id, isManga="", caption=""):
        try:
            c = self.conn.cursor()
            member_id = int(member_id)
            image_id = int(image_id)

            with self.conn.transaction():
                c.execute('''INSERT INTO pixiv_master_image VALUES(%s, %s, 'N/A' ,'N/A' , CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s )''',
                        (image_id, member_id, isManga, caption))
        except BaseException:
            print('Error at insertImage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def insertMangaImages(self, manga_files):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.executemany('''INSERT INTO pixiv_manga_image
                            VALUES(%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''', manga_files)
        except BaseException:
            print('Error at insertMangaImages():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def blacklistImage(self, memberId, ImageId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''INSERT OR REPLACE INTO pixiv_master_image
                        VALUES(%s, %s, '**BLACKLISTED**' ,'**BLACKLISTED**' , CURRENT_TIMESTAMP, CURRENT_TIMESTAMP )''',
                        (ImageId, memberId))
        except BaseException:
            print('Error at blacklistImage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectImageByMemberId(self, member_id):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT * FROM pixiv_master_image WHERE member_id = %s ''', (member_id,))
            return c.fetchall()
        except BaseException:
            print('Error at selectImageByMemberId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectImageByMemberIdAndImageId(self, member_id, image_id):
        try:
            c = self.conn.cursor()
            c.execute('''SELECT image_id FROM pixiv_master_image
                      WHERE image_id = %s AND save_name != 'N/A' AND member_id = %s''', (image_id, member_id))
            return c.fetchone()
        except BaseException:
            print('Error at selectImageByMemberIdAndImageId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectImageByImageId(self, image_id, cols='*'):
        try:
            c = self.conn.cursor()
            c.execute(
                f"SELECT {cols} FROM pixiv_master_image WHERE image_id = %s AND save_name != 'N/A'", (image_id,))
            return c.fetchone()
        except BaseException:
            print('Error at selectImageByImageId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectImageByImageIdAndPage(self, imageId, page):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT * FROM pixiv_manga_image WHERE image_id = %s AND page = %s ''', (imageId, page))
            return c.fetchone()
        except BaseException:
            print('Error at selectImageByImageIdAndPage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def updateImage(self, imageId, title, filename, isManga=None, caption=None):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''UPDATE pixiv_master_image
                        SET title = %s, save_name = %s, last_update_date = CURRENT_TIMESTAMP, is_manga = COALESCE(%s, is_manga), caption = COALESCE(%s, caption)
                        WHERE image_id = %s''', (title, filename, isManga, caption, imageId))
        except BaseException:
            print('Error at updateImage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteImage(self, imageId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''DELETE FROM pixiv_master_image WHERE image_id = %s''', (imageId, ))
                c.execute('''DELETE FROM pixiv_manga_image WHERE image_id = %s''', (imageId, ))
                c.execute('''DELETE FROM pixiv_image_to_tag WHERE image_id = %s''', (imageId, ))
        except BaseException:
            print('Error at deleteImage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteSketch(self, postId):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''DELETE FROM sketch_master_post WHERE post_id = %s''', (postId, ))
                c.execute('''DELETE FROM sketch_post_image WHERE post_id = %s''', (postId, ))
        except BaseException:
            print('Error at deleteSketch():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def checkFilenames(self, base_filename, exts):
        for ext2 in exts:
            check_name = base_filename + ext2
            if os.path.exists(check_name):
                return True
        return False

    def cleanupFileExists(self, filename):
        ''' check if file or converted file exists '''
        anim_ext = ['.zip', '.gif', '.apng', '.ugoira', '.webm']
        fileExists = False
        if filename is not None or len(filename) > 0:
            if os.path.exists(filename):
                return True
            for ext in anim_ext:
                # check filename in db against all combination possible filename in disk
                if filename.endswith(ext):
                    base_filename = filename.rsplit(ext, 1)[0]
                    if self.checkFilenames(base_filename, anim_ext):
                        fileExists = True
                        break
        return fileExists

    def cleanUp(self):
        anim_ext = ['.zip', '.gif', '.apng', '.ugoira', '.webm']
        try:
            print("Start clean-up operation.")
            print("Selecting all images, this may take some times.")
            c = self.conn.cursor()
            c.execute('''SELECT image_id, save_name from pixiv_master_image''')
            print("Checking images.")
            for row in c:
                # Issue 340
                filename = row[1]
                fileExists = False

                if filename is not None and len(filename) > 0:
                    if os.path.exists(filename):
                        continue

                    for ext in anim_ext:
                        # check filename in db against all combination possible filename in disk
                        if filename.endswith(ext):
                            base_filename = filename.rsplit(ext, 1)[0]
                            if self.checkFilenames(base_filename, anim_ext):
                                fileExists = True
                                break

                if not fileExists:
                    print("Missing: {0} at {1}".format(row[0], row[1]))
                    self.deleteImage(row[0]) # with self.conn.transaction():
        except BaseException:
            print('Error at cleanUp():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def interactiveCleanUp(self):
        pass

    def replaceRootPath(self):
        pass

##########################################
# VI. CRUD FANBOX post/image table       #
##########################################

    def insertPost(self, member_id, post_id, title, fee_required, published_date, post_type):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                post_id = int(post_id)
                c.execute(
                    '''INSERT INTO fanbox_master_post (member_id, post_id) VALUES(%s, %s)''',
                    (member_id, post_id))
                c.execute(
                    '''UPDATE fanbox_master_post SET title = %s, fee_required = %s, published_date = %s,
                    post_type = %s, last_update_date = CURRENT_TIMESTAMP WHERE post_id = %s''',
                    (title, fee_required, published_date, post_type, post_id))
        except BaseException:
            print('Error at insertPost():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def insertPostImages(self, post_files):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.executemany('''INSERT OR REPLACE INTO fanbox_post_image
                            VALUES(%s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)''', post_files)
        except BaseException:
            print('Error at insertPostImages():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectPostByPostId(self, post_id):
        try:
            c = self.conn.cursor()
            post_id = int(post_id)
            c.execute(
                '''SELECT * FROM fanbox_master_post WHERE post_id = %s''',
                (post_id,))
            return c.fetchone()
        except BaseException:
            print('Error at selectPostByPostId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def updatePostUpdateDate(self, post_id, updated_date):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                post_id = int(post_id)
                c.execute(
                    '''UPDATE fanbox_master_post SET updated_date = %s
                    WHERE post_id = %s''',
                    (updated_date, post_id))
        except BaseException:
            print('Error at updatePostUpdateDate():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectFanboxImageByImageIdAndPage(self, post_id, page):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT * FROM fanbox_post_image WHERE post_id = %s AND page = %s ''', (post_id, page))
            return c.fetchone()
        except BaseException:
            print('Error at selectFanboxImageByImageIdAndPage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteFanboxPost(self, post_id, by):
        post_id = int(post_id)
        if by not in ["member_id", "post_id"]:
            return

        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute(f'''DELETE FROM fanbox_post_image WHERE post_id in
                            (SELECT post_id FROM fanbox_master_post WHERE {by} = %s)''', (post_id,))
                c.execute(f'''DELETE FROM fanbox_master_post WHERE {by} = %s''', (post_id,))
        except BaseException:
            print('Error at deleteFanboxPost():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def cleanUpFanbox(self):
        print("Start FANBOX clean-up operation.")
        print("Selecting all FANBOX images, this may take some times.")
        items = []
        try:
            c = self.conn.cursor()
            c.execute('''SELECT post_id, page, save_name from fanbox_post_image''')
            print("Checking images.")
            for row in c:
                filename = row[2]

                if filename is not None and len(filename) > 0:
                    if os.path.exists(filename):
                        continue

                print("Missing: {0} at {1}".format(row[0], row[2]))
                items.append(row)

            for item in items:
                with self.conn.transaction():
                    c.execute('''DELETE FROM fanbox_post_image WHERE post_id = %s and page = %s''', (item[0], item[1]))
                    c.execute('''DELETE FROM fanbox_master_post WHERE post_id = %s''', (item[0],))
        except BaseException:
            print('Error at cleanUpFanbox():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def interactiveCleanUpFanbox(self):
        pass

##########################################
# VII. CRUD Sketch post/image table      #
##########################################

    def insertSketchPost(self, post):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                post_id = int(post.imageId)
                c.execute('''INSERT INTO sketch_master_post (member_id, post_id) VALUES(%s, %s)''',
                        (post.artist.artistId, post_id))
                c.execute('''UPDATE sketch_master_post
                                SET title = %s,
                                    published_date = %s,
                                    post_type = %s,
                                    last_update_date = %s
                                WHERE post_id = %s''',
                        (post.imageTitle, post.worksDateDateTime, post.imageMode, post.worksUpdateDateTime, post_id))
        except BaseException:
            print('Error at insertSketchPost():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def insertSketchPostImages(self, post_id, page, save_name, created_date, last_update_date):
        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute('''INSERT OR REPLACE INTO sketch_post_image
                                VALUES(%s, %s, %s, %s, %s)''',
                        (post_id, page, save_name, created_date, last_update_date))
        except BaseException:
            print('Error at insertSketchPostImages():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectSketchImageByImageIdAndPage(self, post_id, page):
        try:
            c = self.conn.cursor()
            c.execute(
                '''SELECT * FROM sketch_post_image WHERE post_id = %s AND page = %s ''', (post_id, page))
            return c.fetchone()
        except BaseException:
            print('Error at selectSketchImageByImageIdAndPage():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def selectSketchPostByPostId(self, post_id):
        try:
            c = self.conn.cursor()
            post_id = int(post_id)
            c.execute(
                '''SELECT * FROM sketch_master_post WHERE post_id = %s''',
                (post_id,))
            return c.fetchone()
        except BaseException:
            print('Error at selectSketchPostByPostId():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def deleteSketchPost(self, post_id, by):
        post_id = int(post_id)
        if by not in ["member_id", "post_id"]:
            return

        try:
            c = self.conn.cursor()
            with self.conn.transaction():
                c.execute(f'''DELETE FROM sketch_post_image WHERE post_id in
                            (SELECT post_id FROM sketch_master_post WHERE {by} = %s)''', (post_id,))
                c.execute(f'''DELETE FROM sketch_master_post WHERE {by} = %s''', (post_id,))
        except BaseException:
            print('Error at deleteSketchPost():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def cleanUpSketch(self):
        try:
            print("Start sketch clean-up operation.")
            print("Selecting all sketches, this may take some times.")
            c = self.conn.cursor()
            c.execute('''SELECT post_id, page, save_name from sketch_post_image''')
            print("Checking images.")
            for row in c:
                # Issue 340
                filename = row[2]
                fileExists = False

                if filename is not None and len(filename) > 0:
                    if os.path.exists(filename):
                        continue

                if not fileExists:
                    print("Missing: {0} at {1}".format(row[0], row[2]))
                    self.deleteSketch(row[0])
        except BaseException:
            print('Error at cleanUpSketch():', str(sys.exc_info()))
            print('failed')
            raise
        finally:
            c.close()

    def interactiveSketchCleanUp(self):
        pass

##########################################
# VIII. CRUD Novel table                 #
##########################################
    def create_update_novel_table(self, c):
        pass

    def insertNovelPost(self, post, filename):
        pass

    def selectNovelPostByPostId(self, post_id):
        pass

##########################################
# VIII. Utilities                        #
##########################################

    def menu(self):
        PADDING = 60
        print(Fore.YELLOW + Back.BLACK + Style.BRIGHT + 'Pixiv DB Manager Console' + Style.RESET_ALL)
        print(Style.BRIGHT + '── Pixiv '.ljust(PADDING, "─") + Style.RESET_ALL)
        print('1. Show all member')
        print('2. Show all images')
        print('3. Export list (member_id only)')
        print('4. Export list (detailed)')
        print('5. Show member by last downloaded date')
        print('6. Show image by image_id')
        print('7. Show member by member_id')
        print('8. Show image by member_id')
        print('9. Delete member by member_id')
        print('10. Delete image by image_id')
        print('11. Delete member and image (cascade deletion)')
        print('12. Blacklist image by image_id')
        print('13. Show all deleted member')
        print('14. Delete members by list')
        print('15. Keep members by list')
        print(Style.BRIGHT + '── FANBOX '.ljust(PADDING, "─") + Style.RESET_ALL)
        print('f1. Export FANBOX post list')
        print('f2. Delete FANBOX download history by member_id')
        print('f3. Delete FANBOX download history by post_id')
        print(Style.BRIGHT + '── Sketch '.ljust(PADDING, "─") + Style.RESET_ALL)
        print('s1. Delete Sketch download history by member_id')
        print('s2. Delete Sketch download history by post_id')
        print(Style.BRIGHT + '── Batch Manage DB '.ljust(PADDING, "─") + Style.RESET_ALL)
        print('c. Clean Up Database')
        print('i. Interactive Clean Up Database')
        print('p. Compact Database')
        print('r. Replace Root Path')
        print('x. Exit')
        selection = input('Select one? ').rstrip("\r")
        return selection

    def main(self):
        PixivHelper.get_logger().info('DB Manager (d).')
        try:
            while True:
                selection = self.menu()

                if selection == '1':
                    self.printMemberList()
                elif selection == '2':
                    self.printImageList()
                elif selection == '3':
                    filename = input('Filename? ').rstrip("\r")
                    includeArtistToken = input(
                        'Include Artist Token[y/n, default is no]? ').rstrip("\r")
                    if includeArtistToken.lower() == 'y':
                        includeArtistToken = True
                    else:
                        includeArtistToken = False
                    self.exportList(filename, includeArtistToken)
                elif selection == '4':
                    filename = input('Filename? ').rstrip("\r")
                    self.exportDetailedList(filename)
                elif selection == '5':
                    date = input('Number of date? ').rstrip("\r")
                    rows = self.selectMembersByLastDownloadDate(date)
                    if rows is not None:
                        for row in rows:
                            print("{0}\t\t{1}\n".format(
                                row.memberId, row.path))
                    else:
                        print('Not Found!\n')
                elif selection == '6':
                    image_id = input('image_id? ').rstrip("\r")
                    row = self.selectImageByImageId(image_id)
                    if row is not None:
                        for string in row:
                            print('	', end=' ')
                            print(string)
                        print('\n')
                    else:
                        print('Not Found!\n')
                elif selection == '7':
                    member_id = input('member_id? ').rstrip("\r")
                    row = self.selectMemberByMemberId(member_id)
                    if row is not None:
                        for string in row:
                            print('	', end=' ')
                            print(string)
                        print('\n')
                    else:
                        print('Not Found!\n')
                elif selection == '8':
                    member_id = input('member_id? ').rstrip("\r")
                    rows = self.selectImageByMemberId(member_id)
                    if rows is not None:
                        for row in rows:
                            for string in row:
                                print('	', end=' ')
                                print(string)
                            print('\n')
                    else:
                        print('Not Found!\n')
                elif selection == '9':
                    member_id = input('member_id? ').rstrip("\r")
                    self.deleteMemberByMemberId(member_id)
                elif selection == '10':
                    image_id = input('image_id? ').rstrip("\r")
                    self.deleteImage(image_id)
                elif selection == '11':
                    member_id = input('member_id? ').rstrip("\r")
                    self.deleteCascadeMemberByMemberId(member_id)
                elif selection == '12':
                    member_id = input('member_id? ').rstrip("\r")
                    image_id = input('image_id? ').rstrip("\r")
                    self.blacklistImage(member_id, image_id)
                elif selection == '13':
                    self.printMemberList(isDeleted=True)
                elif selection == '14':
                    self.deleteMembersByList()
                elif selection == '15':
                    self.keepMembersByList()
                elif selection == 'f1':
                    filename = input('Filename? ').rstrip("\r")
                    sep = input('Separator? (1(default)=",", 2="\\t") ').rstrip("\r")
                    sep = "\t" if sep == "2" else ","
                    self.exportFanboxPostList(filename, sep)
                elif selection == 'f2':
                    member_id = input('member_id? ').rstrip("\r")
                    self.deleteFanboxPost(member_id, "member_id")
                elif selection == 'f3':
                    post_id = input('post_id? ').rstrip("\r")
                    self.deleteFanboxPost(post_id, "post_id")
                elif selection == 's1':
                    member_id = input('member_id? ').rstrip("\r")
                    self.deleteSketchPost(member_id, "member_id")
                elif selection == 's2':
                    post_id = input('post_id? ').rstrip("\r")
                    self.deleteSketchPost(post_id, "post_id")
                elif selection == 'c':
                    self.cleanUp()
                    self.cleanUpFanbox()
                    self.cleanUpSketch()
                elif selection == 'i':
                    self.interactiveCleanUp()
                    self.interactiveCleanUpFanbox()
                    self.interactiveSketchCleanUp()
                elif selection == 'p':
                    self.compactDatabase()
                elif selection == 'r':
                    self.replaceRootPath()
                elif selection == 'x':
                    break
            print('end PixivDBManager.')
        except BaseException:
            print('Error: ', sys.exc_info())
            self.main()
