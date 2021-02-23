from core.aws.aws_config import AWSConfig
from core.aws.s3.aws_s3 import existing_on_s3
from core.crud.sql.datasource import get_all_datasource_by_ids, get_one_datasource_by_id
from core.crud.sql.track import get_one_track_by_id
from google_spreadsheet_api.function import get_df_from_speadsheet, get_gsheet_name
from core.models.data_source_format_master import DataSourceFormatMaster
from core import query_path
from tools.crawlingtask import crawl_youtube, WhenExist
import pandas as pd
import time


def get_split_info(vibbidi_title: str, track_title: str):
    k = vibbidi_title.replace(track_title, "").strip()[1:-1]
    raw_year = k.split(' ')[-1]
    if raw_year.isnumeric():
        year = raw_year
        concert_live_name = k.replace(year, "")
    else:
        year = ''
        concert_live_name = k
    return {"year": year, "concert_live_name": concert_live_name}


def checking_lost_filename_datasource_from_S3(datasource_id: str):
    db_datasource = get_one_datasource_by_id(datasource_id)

    if "berserker" in db_datasource.cdn:
        key = f"videos/{db_datasource.file_name}"
    else:
        key = f"audio/{db_datasource.file_name}"
    result = existing_on_s3(key)
    print(f"{key}---{AWSConfig.S3_DEFAULT_BUCKET}")
    print(f"Datasource id: [{db_datasource.id}] - {result}")
    return result


def proccess_file_name_lost_from_S3(datasource_ids: list):
    print("start checking_lost_datasource_from_S3")
    db_datasources = get_all_datasource_by_ids(datasource_ids)
    for db_datasource in db_datasources:
        # step 1: check lost file from datasourceid
        if "berserker" in db_datasource.cdn:
            key = f"videos/{db_datasource.file_name}"
        else:
            key = f"audio/{db_datasource.file_name}"
        result = existing_on_s3(key)
        print(f"{key}---{AWSConfig.S3_DEFAULT_BUCKET}")
        print(f"Datasource id: [{db_datasource.id}] - {result}")

        # Step 2: get datasource lost from S3
        if result == 0:
            with open('/Users/phamhanh/PycharmProjects/data_operation_fixed1/sources/datasource_id', "a+") as f1:
                if db_datasource.track_id == '':
                    joy_xinh_qua = f"datasource not have trackid-------{db_datasource.id}-------{db_datasource.format_id}-------{db_datasource.source_uri};\n"
                    f1.write(joy_xinh_qua)
                else:
                    db_track = get_one_track_by_id(db_datasource.track_id)
                    if not db_track:
                        joy_xinh_qua = f"not existed track_valid-------{db_datasource.id}-------{db_datasource.format_id}-------{db_datasource.source_uri};\n"
                        f1.write(joy_xinh_qua)
                    else:
                        joy_xinh_qua = f"{db_track.id}-------{db_datasource.id}-------{db_datasource.format_id}-------{db_datasource.source_uri};\n"
                        f1.write(joy_xinh_qua)

                        # Step 3: to fix datasource lost from S3
                        with open(query_path, "a+") as f2:
                            if db_datasource.format_id in (
                                    DataSourceFormatMaster.FORMAT_ID_MP3_FULL,
                                    DataSourceFormatMaster.FORMAT_ID_MP4_FULL,
                                    DataSourceFormatMaster.FORMAT_ID_MP4_STATIC,
                                    DataSourceFormatMaster.FORMAT_ID_MP4_LYRIC):
                                joy_xinh = crawl_youtube(track_id=db_track.id, youtube_url=db_datasource.source_uri,
                                                         format_id=db_datasource.format_id,
                                                         when_exist=WhenExist.REPLACE,
                                                         pic=f"{gsheet_name}_{sheet_name}", priority=1999)
                                f2.write(joy_xinh)
                            elif result == 0 and db_datasource.format_id == DataSourceFormatMaster.FORMAT_ID_MP4_LIVE:
                                vibbidi_title = db_datasource.info.get('vibbidi_title')
                                track_title = db_track.title
                                live_info = get_split_info(vibbidi_title=vibbidi_title, track_title=track_title)
                                joy_xinh = crawl_youtube(track_id=db_track.id, youtube_url=db_datasource.source_uri,
                                                         format_id=db_datasource.format_id,
                                                         when_exist=WhenExist.REPLACE,
                                                         place=live_info.get('concert_live_name'),
                                                         year=live_info.get('year'),
                                                         pic=f"{gsheet_name}_{sheet_name}",
                                                         priority=1999)
                                f2.write(joy_xinh)
                            else:
                                continue


def checking_lost_datasource_resize_image_from_S3(datasource_id: str):
    db_datasource = get_one_datasource_by_id(datasource_id)
    ext_keys = db_datasource.ext.keys()
    resize_image_types = ["micro", "tiny", "small", "medium", "large", "extra"]
    result = ""
    with open(query_path, "a+") as f:
        if "resize_images" in ext_keys:
            for resize_image_type in resize_image_types:
                existed_resize_images = db_datasource.ext.get('resize_images')
                loop = False
                joy_xinh = ""
                for existed_resize_image in existed_resize_images:
                    existed_resize_image_type = existed_resize_image.split(".")[-2]
                    if existed_resize_image_type == resize_image_type:
                        loop = True
                        if "video" in existed_resize_image:
                            key = f"videos/{existed_resize_image}"
                        else:
                            key = f"audio/{existed_resize_image}"
                        existed_S3_resize_image = existing_on_s3(key, bucket=AWSConfig.S3_DEFAULT_BUCKET)
                        joy_xinh = joy_xinh + f"{db_datasource.id}, {resize_image_type}, {existed_S3_resize_image}, {key}, {db_datasource.source_uri}\n"
                        if not existed_S3_resize_image:
                            f.write(joy_xinh)
                        break
                    else:
                        continue
                result = result + joy_xinh
                if not loop:
                    joy_xinh = joy_xinh + f"{db_datasource.id}, {resize_image_type}, not have, not have, not have\n"
                    result = result + f"{db_datasource.id}, {resize_image_type}, not have, not have, not have\n"
                    f.write(joy_xinh)
        else:
            result = result + f"{db_datasource.id}, not have, not have, not have, not have\n"
            joy_xinh =f"{db_datasource.id}, not have, not have, not have, not have\n"
            f.write(joy_xinh)
        print(result)


def checking_fault_datasource_image(datasource_id: str):
    db_datasource = get_one_datasource_by_id(datasource_id)
    if "berserker" in db_datasource.cdn:
        key = f"videos/{db_datasource.file_name}.jpg"
    else:
        key = f"audio/{db_datasource.file_name}.jpg"
    result = existing_on_s3(key)
    print(f"{key}---{AWSConfig.S3_DEFAULT_BUCKET}")
    print(f"Datasource id: [{db_datasource.id}] - {result}")
    return result

if __name__ == "__main__":
    # https://docs.google.com/spreadsheets/d/1Qu5oUocflDr4ERJvux8eSnuVVIGp1-WNzjqE7NeYKJI/edit#gid=709402142

    start_time = time.time()
    gsheetid = '1Qu5oUocflDr4ERJvux8eSnuVVIGp1-WNzjqE7NeYKJI'
    gsheet_name = get_gsheet_name(gsheet_id=gsheetid)
    sheet_name = 'checking lost resize image from S3'
    df = get_df_from_speadsheet(gsheet_id=gsheetid, sheet_name=sheet_name)
    list_dsid = list(dict.fromkeys(df['datasourceid'].values.tolist()))
    for dsid in list_dsid:
        # print(dsid + "\n")
        # checking_lost_datasource_resize_image_from_S3(dsid)
        checking_fault_datasource_image(dsid)
    # proccess_file_name_lost_from_S3(list_dsid)
    # list_dsid = [
    #     "F3ED1BFEB02E451188351CF0802429E7",
    #     "2D578249F2C949F6AE0AA9AB20159804",
    #     "B197B967140C4E22ABD7D6588F82BD14",
    #     "D50B2CE3245740349418B4D1653F78A0",
    #     "C8DCD912FCAE438483B4C0A610E1FC7F"
    # ]
    print("\n --- %s seconds ---" % (time.time() - start_time))
