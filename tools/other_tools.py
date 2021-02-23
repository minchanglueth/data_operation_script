from core.aws.aws_config import AWSConfig
from core.aws.s3.aws_s3 import existing_on_s3
from core.crud.sql.datasource import get_all_datasource_by_ids, get_one_datasource_by_id, related_datasourceid

from core.crud.sql.track import get_one_track_by_id
from google_spreadsheet_api.function import get_df_from_speadsheet, get_gsheet_name
from open_cv_function.openCV_simple import get_video_duration
from tools.checking_lost_file_from_S3 import checking_lost_datasource_from_S3

from core.models.data_source_format_master import DataSourceFormatMaster
from core import query_path
from tools.crawlingtask import crawl_youtube, WhenExist
import time


def update_datasource_duration(datasource_ids: list):
    # step 1: S3_filename_url
    for datasourceid in list_dsid:
        db_datasource = get_one_datasource_by_id(datasourceid=datasourceid)
        if "berserker" in db_datasource.cdn:
            s3_filename_url = "https://s3.amazonaws.com/vibbidi-us/videos/" + db_datasource.file_name
        else:
            s3_filename_url = "https://s3.amazonaws.com/vibbidi-us/audio/" + db_datasource.file_name
        print(s3_filename_url)
        # step 2 get datasource duration
        result = checking_lost_datasource_from_S3(db_datasource.id)
        if not result:
            print(f"datasource_id: {db_datasource.id} lost filename from S3")
        else:
            datasource_duration = get_video_duration(s3_filename_url)
            with open(query_path, "a+") as f:
                joy_xinh = f"Update datasources set DurationMs = {datasource_duration} where id = '{db_datasource.id}';\n"
                f.write(joy_xinh)


def remove_datasource(datasourceid: str):
    db_datasource = get_one_datasource_by_id(datasourceid=datasourceid)
    related_id_datasource = related_datasourceid(datasourceid)
    query = ""
    if related_id_datasource == [(None, None, None)]:
        query = query + f"Update datasources set valid = -10 where id = {datasourceid};"
    else:
        print("-- related_id_datasource = true")
        if not db_datasource.track_id:
            query = query + f"UPDATE datasources SET trackid = '', FormatID = ''  where id = '{datasourceid}';\n"
        else:
            query = query + f"UPDATE datasources SET trackid = '', FormatID = ''  where id = '{datasourceid}';\n"
            query = query + f"UPDATE datasources SET updatedAt = NOW() WHERE trackid = '{db_datasource.track_id}';\n"
    print(query)
    return query


if __name__ == "__main__":
    # https://docs.google.com/spreadsheets/d/1Qu5oUocflDr4ERJvux8eSnuVVIGp1-WNzjqE7NeYKJI/edit#gid=709402142
    start_time = time.time()
    gsheetid = '1Qu5oUocflDr4ERJvux8eSnuVVIGp1-WNzjqE7NeYKJI'
    gsheet_name = get_gsheet_name(gsheet_id=gsheetid)
    sheet_name = 'lost duration'
    df = get_df_from_speadsheet(gsheet_id=gsheetid, sheet_name=sheet_name)
    list_dsid = list(dict.fromkeys(df['datasourceid'].values.tolist()))
    # print(list_dsid)

    update_datasource_duration(list_dsid)


    print("\n --- %s seconds ---" % (time.time() - start_time))
