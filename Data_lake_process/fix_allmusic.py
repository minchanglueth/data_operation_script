from functools import update_wrapper
from sqlalchemy import exc
from hashlib import new
from gspread.models import Worksheet
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import numpy as np
from gspread_pandas import Spread, Client
from gspread_formatting.dataframe import format_with_dataframe
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import update
from google_spreadsheet_api.gspread_utility import gc, get_df_from_gsheet
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import and_, func
from core.mysql_database_connection.sqlalchemy_create_engine import (
    SQLALCHEMY_DATABASE_URI,
)
from core.crud.sql import insert_ignore
from core.models.crawlingtask import Crawlingtask
from core.models.chart_album import ChartAlbum
from core.models.album_track import Album_Track
from core.models.album import Album
from core.models.artist_album import Artist_album
from core.models.external_identity import ExternalIdentity
from core.models.itunes_album_tracks_release import ItunesRelease
from core.models.pointlog import PointLog
from core.models.collection_album import CollectionAlbum
from core.models.related_album import RelatedAlbum
from core.models.theme_album import ThemeAlbum
from core.models.urimapper import URIMapper
from core.models.reportautocrawler_top100albums import ReportAutoCrawlerTop100Album
from core.models.sg_likes import SgLikes
from core.models.album_contributor import AlbumContributor
from core.models.usernarrative import UserNarrative
from core.models.albumcountlog import AlbumCountLog
from core.crud.get_df_from_query import get_df_from_query
import pandas as pd
from uuid import uuid4
import time
import re

engine = create_engine(SQLALCHEMY_DATABASE_URI, pool_pre_ping=True)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine)
)


def get_number_from_string(string_with_number):
    number = re.sub(r"\D", "", string_with_number)
    if number != "":
        number = int(number)
    if number == "":
        number = np.nan
    return number


def get_ituneid(gsheet_url: str, sheet_name: str):
    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.worksheet(sheet_name)
    df = get_as_dataframe(
        worksheet,
        header=1,
        skip_blank_lines=True,
        evaluate_formulas=True,
        dtype={"Apple ID": str, "Recheck ID": str},
    )
    dff = df[(df["Apple ID"].notnull()) & (df["Apple ID"] != "")][
        [
            "albumuuid",
            "artistuuid",
            "Album Title",
            "Artist Name",
            "Apple ID",
            "Recheck ID",
            "Region",
        ]
    ]
    dff.columns = [
        "albumuuid_old",
        "artistuuid_old",
        "album_title",
        "artist_name",
        "apple_id",
        "recheck_id",
        "region",
    ]
    dff = dff[dff.recheck_id == "ok"].copy()
    dff["apple_id"] = dff["apple_id"].apply(get_number_from_string)
    old_album_uuids = dff.albumuuid_old.values.tolist()
    query_album_id = db_session.query(Album.id, Album.uuid).filter(
        Album.uuid.in_(old_album_uuids)
    )
    df_old_id = get_df_from_query(query_album_id)
    db_session.close()
    dff_merged = pd.merge(dff, df_old_id, left_on="albumuuid_old", right_on="uuid")
    dff_merged = dff_merged[
        [
            "albumuuid_old",
            "artistuuid_old",
            "album_title",
            "artist_name",
            "apple_id",
            "id",
            "region",
        ]
    ]
    dff_merged.columns = [
        "albumuuid_old",
        "artistuuid_old",
        "album_title",
        "artist_name",
        "apple_id",
        "albumid_old",
        "region",
    ]
    return dff_merged


def print_old_info(df, gsheet_url: str, sheet_name: str):
    albumuuid_list = df[df.albumid_old.notnull()]["albumuuid_old"].values.tolist()
    query_album = (
        db_session.query(
            Album.uuid.label("AlbumUUID (old)"),
            Album.info.label("Albums.info"),
            Artist_album.album_id.label("Artist_album.albumID"),
            CollectionAlbum.album_id.label("Collection_album.albumID"),
            ThemeAlbum.album_id.label("Theme_album.albumID"),
            URIMapper.entity_id.label("Urimapper.entityID"),
            SgLikes.entity_uuid.label("sg_likes.entityUUID"),
            AlbumContributor.album_id.label("Albumcontributors.albumID"),
            UserNarrative.entity_uuid.label("Usernarratives.entityUUID"),
            ItunesRelease.album_uuid.label("itunes_album_tracks_release.AlbumUUID"),
            AlbumCountLog.album_uuid.label("albumcountlog.AlbumUUID"),
            Album.valid.label("Albums.valid"),
            ItunesRelease.valid.label("ItunesRelease.valid"),
        )
        .select_from(Album)
        .join(Artist_album, Artist_album.album_id == Album.id)
        .join(CollectionAlbum, CollectionAlbum.album_id == Album.uuid, isouter=True)
        .join(ThemeAlbum, ThemeAlbum.album_id == Album.id, isouter=True)
        .join(URIMapper, URIMapper.entity_id == Album.uuid, isouter=True)
        .join(SgLikes, SgLikes.entity_uuid == Album.uuid, isouter=True)
        .join(AlbumContributor, AlbumContributor.album_id == Album.uuid, isouter=True)
        .join(UserNarrative, UserNarrative.entity_uuid == Album.uuid, isouter=True)
        .join(ItunesRelease, ItunesRelease.album_uuid == Album.uuid, isouter=True)
        .join(AlbumCountLog, AlbumCountLog.album_uuid == Album.uuid, isouter=True)
        .filter(Album.uuid.in_(albumuuid_list))
        .distinct()
    )
    album_df = get_df_from_query(query_album).drop_duplicates(
        subset=["AlbumUUID (old)"]
    )
    db_session.close()
    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.worksheet(sheet_name)
    sheet_df = get_as_dataframe(
        worksheet,
        header=1,
        skip_blank_lines=True,
        evaluate_formulas=True,
    )
    sheet_df = sheet_df[["Itune ID (new)", "AlbumUUID (old)"]]
    df = pd.merge(
        sheet_df,
        album_df,
        how="left",
        left_on="AlbumUUID (old)",
        right_on="AlbumUUID (old)",
    ).drop_duplicates(subset=["AlbumUUID (old)"])
    set_with_dataframe(worksheet, df, include_column_header=False, row=3)


def run_crawler(df: object):
    crawlers = df[["apple_id", "region"]].copy()
    crawler_actionid = "9C8473C36E57472281A1C7936108FC06"
    crawler_id_list = []
    insert_list = []
    for row in crawlers.index:
        region = str(df.at[row, "region"])
        id = str(df.at[row, "apple_id"])
        task_detail = {"PIC": "Maddie-allmusic", "region": region, "album_id": id}
        crawler_id = str(uuid4()).upper().replace("-", "")
        c = Crawlingtask(
            id=crawler_id,
            priority=1205,
            actionid=crawler_actionid,
            taskdetail=task_detail,
        )
        crawler_id_list.append(crawler_id)
        insert_list.append(c)
    try:
        db_session.bulk_save_objects(insert_list)
        db_session.commit()
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()
    return crawler_id_list


def get_complete_crawl(id_list: list):
    try:
        query_crawler = (
            db_session.query(
                Crawlingtask.id,
                func.json_extract(Crawlingtask.taskdetail, "$.album_id").label(
                    "itune_id"
                ),
            )
            .select_from(Crawlingtask)
            .filter(Crawlingtask.id.in_(id_list))
            .order_by(Crawlingtask.created_at.desc())
        )

        dfc = get_df_from_query(query_crawler).drop_duplicates(subset="itune_id")
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()

    itunes_id_list = (
        dfc[dfc["itune_id"].notnull()]["itune_id"]
        .str.replace('"', "")
        .astype(float)
        .astype(int)
        .values.tolist()
    )
    query_itune_id = db_session.query(
        Album.id, Album.uuid, Album.external_id, Album.info
    ).filter(Album.external_id.in_(itunes_id_list))

    db_session.close()
    return query_itune_id


def get_all_crawl(id_list: list, sheet_name: str, gsheet_url: str):
    try:
        query_crawler = (
            db_session.query(
                func.json_extract(Crawlingtask.taskdetail, "$.album_id").label(
                    "itune_id"
                ),
                Crawlingtask.id.label("Crawlingtask.ID"),
                Crawlingtask.status.label("Crawlingtask.status"),
            )
            .select_from(Crawlingtask)
            .filter(Crawlingtask.id.in_(id_list))
            .order_by(Crawlingtask.created_at.desc())
        )

        dfc = get_df_from_query(query_crawler).drop_duplicates(subset="itune_id")

    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()

    dfc["itune_id"] = dfc["itune_id"].str.replace('"', "").astype(int)
    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.worksheet(sheet_name)
    sheet_df = get_as_dataframe(
        worksheet, header=1, skip_blank_lines=True, evaluate_formulas=True
    )
    sheet_df["Itune ID (new)"] = (
        sheet_df["Itune ID (new)"].astype(str).apply(get_number_from_string)
    )
    new_sheet_df = pd.merge(
        sheet_df, dfc, left_on="Itune ID (new)", right_on="itune_id"
    )
    new_sheet_df["CrawlingtaskID"] = new_sheet_df["Crawlingtask.ID"]
    new_sheet_df["Crawl Result"] = new_sheet_df["Crawlingtask.status"]
    del new_sheet_df["Crawlingtask.ID"]
    del new_sheet_df["Crawlingtask.status"]
    new_sheet_df = new_sheet_df.iloc[:, :-1]
    set_with_dataframe(
        worksheet=worksheet, dataframe=new_sheet_df, row=3, include_column_header=False
    )


def get_incomplete_crawl(gsheet_url: str, id_list: list):
    try:
        query = (
            db_session.query(
                Crawlingtask.id.label("Crawlingtask_id"),
                Crawlingtask.status,
                Crawlingtask.taskdetail,
                func.json_extract(Crawlingtask.taskdetail, "$.album_id").label(
                    "itune_id"
                ),
            )
            .select_from(Crawlingtask)
            .filter(Crawlingtask.id.in_(id_list), Crawlingtask.status != "complete")
        )
        df = get_df_from_query(query)
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()

    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.worksheet("Incomplete crawling tasks")
    df_ = get_as_dataframe(worksheet, skip_blank_lines=True)
    dff = pd.concat([df, df_]).drop_duplicates(subset="Crawlingtask_id")
    set_with_dataframe(worksheet, dff)


def get_new_album_id(query):
    df = get_df_from_query(query)
    new_album_ids = df["id"].values.tolist()
    return new_album_ids


def get_new_album_uuid(query):
    df = get_df_from_query(query)
    new_album_uuids = df["uuid"].values.tolist()
    return new_album_uuids


def merge_new_old_ids(query, df):
    try:
        new_df = get_df_from_query(query)
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()

    merged_df = pd.merge(df, new_df, left_on="apple_id", right_on="external_id")
    merged_df = merged_df[
        [
            "albumuuid_old",
            "albumid_old",
            "artistuuid_old",
            "album_title",
            "artist_name",
            "apple_id",
            "id",
            "uuid",
        ]
    ]
    merged_df.columns = [
        "albumuuid_old",
        "albumid_old",
        "artistuuid_old",
        "album_title",
        "artist_name",
        "apple_id",
        "albumid_new",
        "uuid_new",
    ]
    return merged_df


def update_albums(merged_df):
    for row in merged_df.index:
        old_uuid = str(merged_df.at[row, "albumuuid_old"])
        old_id = int(merged_df["albumid_old"].loc[row])
        new_uuid = str(merged_df.at[row, "uuid_new"])
        new_id = int(merged_df.at[row, "albumid_new"])
        try:
            # set valid artist album
            aa = (
                db_session.query(Artist_album)
                .filter(Artist_album.album_id == old_id)
                .update({"valid": -94})
            )

            # update album_id in chart_album
            db_session.query(ChartAlbum).filter(ChartAlbum.album_id == old_id).update(
                {ChartAlbum.album_id: new_id}
            )
            c_list = (
                db_session.query(ChartAlbum).filter(ChartAlbum.album_id == old_id).all()
            )
            for c in c_list:
                db_session.delete(c)

            # update collection_album
            db_session.query(CollectionAlbum).filter(
                CollectionAlbum.album_id == old_uuid
            ).update({CollectionAlbum.album_id: new_uuid})
            ca_list = (
                db_session.query(CollectionAlbum)
                .filter(CollectionAlbum.album_id == old_uuid)
                .all()
            )
            for ca in ca_list:
                db_session.delete(ca)

            # update related_albums
            db_session.query(RelatedAlbum).filter(
                RelatedAlbum.album_id == old_uuid
            ).update({RelatedAlbum.album_id: new_uuid})
            ra_list = (
                db_session.query(RelatedAlbum)
                .filter(RelatedAlbum.album_id == old_uuid)
                .all()
            )
            for ra in ra_list:
                db_session.delete(ra)

            db_session.query(RelatedAlbum).filter(
                RelatedAlbum.related_album_id == old_uuid
            ).update({RelatedAlbum.related_album_id: new_uuid})
            raa_list = (
                db_session.query(RelatedAlbum)
                .filter(RelatedAlbum.related_album_id == old_uuid)
                .all()
            )
            for raa in raa_list:
                db_session.delete(raa)

            # update theme_album
            db_session.query(ThemeAlbum).filter(ThemeAlbum.album_id == old_id).update(
                {"album_id": new_uuid, "valid": -94}
            )

            # update urimapper
            db_session.query(URIMapper).filter(URIMapper.entity_id == old_uuid).update(
                {"entity_id": new_uuid}, synchronize_session=False
            )

            # update report autocrawler top 100
            update_report = (
                db_session.query(ReportAutoCrawlerTop100Album.ext)
                .filter(
                    func.json_extract(ReportAutoCrawlerTop100Album.ext, "$.album_uuid")
                    == old_uuid
                )
                .all()
            )
            for u in update_report:
                u.ext["album_uuid"] = new_uuid

            # update sg_likes
            db_session.query(SgLikes).filter(SgLikes.entity_uuid == old_uuid).update(
                {SgLikes.entity_uuid: new_uuid}
            )

            # update album_contributors
            db_session.query(AlbumContributor).filter(
                AlbumContributor.album_id == old_uuid
            ).update({AlbumContributor.album_id: new_uuid})

            # update user_narratives
            db_session.query(UserNarrative).filter(
                UserNarrative.entity_uuid == old_uuid
            ).update({UserNarrative.entity_uuid: new_uuid})

            # update albums
            info = (
                db_session.query(Album.info)
                .filter(Album.uuid == old_uuid)
                .order_by(Album.updated_at.desc())
                .first()
            )
            db_session.query(Album).filter(Album.uuid == new_uuid).update(
                {Album.info: info}
            )
            db_session.query(Album).filter(Album.uuid == old_uuid).update(
                {"valid": -94}
            )
            db_session.query(Album).filter(Album.uuid == new_uuid).update({"valid": 1})

            db_session.query(ItunesRelease).filter(
                ItunesRelease.album_uuid == old_uuid
            ).update({"valid": -94})

            db_session.query(Artist_album).filter(
                Artist_album.album_id == old_id
            ).update({"valid": -94})

            db_session.commit()

        except exc.SQLAlchemyError as e:
            print(e)
            db_session.rollback()
            raise
            continue
        finally:
            db_session.close()


def update_new_info(id_list: list, merged_df, gsheet_url: str, sheet_name: str):
    new_uuid_list = merged_df["uuid_new"].values.tolist()
    try:
        query_new_info = (
            db_session.query(
                Album.external_id.label("itune_id"),
                Album.uuid.label("AlbumUUID (new)"),
                Album.valid.label("Albums Valid (new)"),
                Artist_album.album_id.label("Artist_album.albumID (new)"),
                ChartAlbum.album_id.label("Chart_album.albumID (new)"),
                CollectionAlbum.album_id.label("Collection_album.albumID (new)"),
                RelatedAlbum.album_id.label("Relatedalbum.albumID (new)"),
                RelatedAlbum.related_album_id.label(
                    "Relatedalbum.relatedalbumID (new)"
                ),
                ThemeAlbum.album_id.label("Theme_album.albumID (new)"),
                URIMapper.entity_id.label("Urimapper.entityID (new)"),
                SgLikes.entity_type.label("sg_likes.entityUUID (new)"),
                AlbumContributor.album_id.label("Albumcontributors.albumID (new)"),
                UserNarrative.entity_uuid.label("Usernarratives.entityUUID (new)"),
                Album.info.label("Albums.info (new)"),
            )
            .select_from(Album)
            .join(Artist_album, Album.id == Artist_album.album_id, isouter=True)
            .join(ChartAlbum, ChartAlbum.album_id == Album.id, isouter=True)
            .join(CollectionAlbum, CollectionAlbum.album_id == Album.uuid, isouter=True)
            .join(RelatedAlbum, RelatedAlbum.album_id == Album.uuid, isouter=True)
            .join(ThemeAlbum, ThemeAlbum.album_id == Album.id, isouter=True)
            .join(URIMapper, URIMapper.entity_id == Album.uuid, isouter=True)
            .join(SgLikes, SgLikes.entity_uuid == Album.uuid, isouter=True)
            .join(
                AlbumContributor, AlbumContributor.album_id == Album.uuid, isouter=True
            )
            .join(UserNarrative, UserNarrative.entity_uuid == Album.uuid, isouter=True)
            .filter(Album.uuid.in_(new_uuid_list))
        )

        new_info_df = get_df_from_query(query_new_info)
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()

    new_info = pd.merge(
        merged_df,
        new_info_df,
        how="left",
        left_on="uuid_new",
        right_on="AlbumUUID (new)",
    )
    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.worksheet(sheet_name)
    sheet_df = get_as_dataframe(worksheet, header=1, skip_blank_lines=True)[
        ["Itune ID (new)", "AlbumUUID (old)"]
    ]
    df = pd.merge(
        sheet_df,
        new_info,
        how="left",
        left_on="AlbumUUID (old)",
        right_on="albumuuid_old",
    )
    df = df[
        [
            "AlbumUUID (new)",
            "Albums Valid (new)",
            "Artist_album.albumID (new)",
            "Chart_album.albumID (new)",
            "Collection_album.albumID (new)",
            "Relatedalbum.albumID (new)",
            "Relatedalbum.relatedalbumID (new)",
            "Theme_album.albumID (new)",
            "Urimapper.entityID (new)",
            "sg_likes.entityUUID (new)",
            "Albumcontributors.albumID (new)",
            "Usernarratives.entityUUID (new)",
            "Albums.info (new)",
        ]
    ]
    set_with_dataframe(worksheet, df, row=3, col=17, include_column_header=False)


if __name__ == "__main__":
    # input here
    gsheet_url = "https://docs.google.com/spreadsheets/d/1H0t9xq2vUesfpBQoieJP6TxHbWl8D43s5kiAZ7K3Cgc/edit#gid=978085935"
    # sheet name of allmusic input data
    sheet_allmusic = "Test_1"
    # sheet name of result check sheet
    sheet_check_result = "Test_2 (check_result)"

    try:
        # actual script part
        df = get_ituneid(gsheet_url, sheet_allmusic)
        print_old_info(df, gsheet_url, sheet_check_result)
        id_list = run_crawler(df)
        time.sleep(300)
        query_complete_crawl = get_complete_crawl(id_list)
        get_all_crawl(id_list, sheet_check_result, gsheet_url)
        merged_df = merge_new_old_ids(query_complete_crawl, df)
        update_albums(merged_df=merged_df)
        get_incomplete_crawl(gsheet_url=gsheet_url, id_list=id_list)
        update_new_info(
            id_list=id_list,
            merged_df=merged_df,
            gsheet_url=gsheet_url,
            sheet_name=sheet_check_result,
        )
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.close()
