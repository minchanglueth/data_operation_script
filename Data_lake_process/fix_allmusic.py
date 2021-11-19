from ast import Str
from functools import update_wrapper
from more_itertools import sliced
import json
from sqlalchemy import exc, cast
from hashlib import new
from gspread.models import Worksheet
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import numpy as np
from gspread_pandas import Spread, Client
from gspread_formatting.dataframe import format_with_dataframe
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import update, case
from sqlalchemy.sql.sqltypes import JSON, String
from sqlalchemy.orm.attributes import flag_modified
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
# db_session = scoped_session(
#     sessionmaker(autocommit=False, autoflush=False, bind=engine)
# )


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
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
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


def print_old_info(df, gsheet_url: str, sheet_name: str, index_slice):
    albumuuid_list = df[df.albumid_old.notnull()]["albumuuid_old"].values.tolist()
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    query_album = (
        db_session.query(
            Album.uuid.label("AlbumUUID (old)"),
            Album.info.label("Albums.info"),
            Artist_album.album_id.label("Artist_album.albumID"),
            ChartAlbum.album_id.label("Chart_album.albumid"),
            CollectionAlbum.album_id.label("Collection_album.albumID"),
            RelatedAlbum.album_id.label("Relatedalbums.albumid"),
            ThemeAlbum.album_id.label("Theme_album.albumID"),
            URIMapper.entity_id.label("Urimapper.entityID"),
            SgLikes.entity_uuid.label("sg_likes.entityUUID"),
            AlbumContributor.album_id.label("Albumcontributors.albumID"),
            ReportAutoCrawlerTop100Album.ext["album_uuid"].label(
                "reportautocrawler_top100albums.ext->>'$.album_uuid'"
            ),
            UserNarrative.entity_uuid.label("Usernarratives.entityUUID"),
            Album.valid.label("Albums.valid"),
            ItunesRelease.valid.label("ItunesRelease.valid"),
        )
        .select_from(Album)
        .join(Artist_album, Artist_album.album_id == Album.id, isouter=True)
        .join(ChartAlbum, ChartAlbum.album_id == Album.id, isouter=True)
        .join(CollectionAlbum, CollectionAlbum.album_id == Album.uuid, isouter=True)
        .join(RelatedAlbum, RelatedAlbum.album_id == Album.uuid, isouter=True)
        .join(ThemeAlbum, ThemeAlbum.album_id == Album.id, isouter=True)
        .join(URIMapper, URIMapper.entity_id == Album.uuid, isouter=True)
        .join(SgLikes, SgLikes.entity_uuid == Album.uuid, isouter=True)
        .join(AlbumContributor, AlbumContributor.album_id == Album.uuid, isouter=True)
        .join(
            ReportAutoCrawlerTop100Album,
            func.json_unquote(ReportAutoCrawlerTop100Album.ext["album_uuid"])
            == Album.uuid,
            isouter=True,
        )
        .join(UserNarrative, UserNarrative.entity_uuid == Album.uuid, isouter=True)
        .join(ItunesRelease, ItunesRelease.album_uuid == Album.uuid, isouter=True)
        .filter(Album.uuid.in_(albumuuid_list))
        .distinct()
    )
    album_df = get_df_from_query(query_album).drop_duplicates(
        subset=["AlbumUUID (old)", "Albums.valid"]
    )
    db_session.close()
    sh = gc.open_by_url(gsheet_url)
    worksheet = sh.worksheet(sheet_name)
    set_with_dataframe(
        worksheet, album_df, include_column_header=False, row=index_slice[0] + 3, col=2
    )


def run_crawler(df: object):
    crawlers = df[["apple_id", "region"]].copy()
    crawler_actionid = "9C8473C36E57472281A1C7936108FC06"
    crawler_id_list = []
    insert_list = []
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
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
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
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


def get_all_crawl(df, id_list: list, sheet_name: str, gsheet_url: str, index_slice):
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
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
        worksheet, header=1, skip_blank_lines=True
    )
    sheet_df = sheet_df.iloc[index_slice]
    new_sheet_df = pd.merge(
        df, dfc, how="left", left_on="apple_id", right_on="itune_id"
    )
    ns_df = pd.merge(sheet_df, new_sheet_df, how="left", left_on="AlbumUUID (old)", right_on="albumuuid_old")
    ns_df_ = ns_df[["Crawlingtask.ID", "Crawlingtask.status"]]
    set_with_dataframe(
        worksheet=worksheet,
        dataframe=ns_df_,
        row=index_slice[0] + 3,
        col=16,
        include_column_header=False,
    )
    set_with_dataframe(
        worksheet=worksheet,
        dataframe=new_sheet_df[["itune_id"]],
        row=index_slice[0] + 3,
        col=1,
        include_column_header=False,
    )
    


def get_incomplete_crawl(gsheet_url: str, id_list: list):
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
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
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    try:
        new_df = get_df_from_query(query)
    except exc.SQLAlchemyError:
        db_session.rollback()
        raise
    finally:
        db_session.flush()
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
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session2 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session3 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session4 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session5 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session6 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session7 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session8 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session9 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session10 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    db_session11 = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
    old_uuid_list = merged_df.albumuuid_old.values.tolist()
    old_id_list = merged_df.albumid_old.values.tolist()
    new_uuid_list = merged_df.uuid_new.values.tolist()
    uuid_dict = dict(zip(merged_df.albumuuid_old, merged_df.uuid_new))
    id_dict = dict(zip(merged_df.albumid_old, merged_df.albumid_new))

    # chart_album
    db_session2.query(ChartAlbum).filter(ChartAlbum.album_id.in_(id_dict)).update(
        {ChartAlbum.album_id: case(id_dict, value=ChartAlbum.album_id)},
        synchronize_session=False,
    )
    c_list = db_session2.query(ChartAlbum).filter(ChartAlbum.album_id.in_(old_id_list))
    for c in c_list:
        db_session2.delete(c)
    db_session2.commit()

    # artist_album
    valid_artist_album = db_session.query(Artist_album).filter(
        Artist_album.album_id.in_(old_id_list)
    )
    for row in valid_artist_album:
        row.valid = -94
    db_session.commit()

    # update collection_album

    db_session3.query(CollectionAlbum).filter(
        CollectionAlbum.album_id.in_(uuid_dict)
    ).update(
        {CollectionAlbum.album_id: case(uuid_dict, value=CollectionAlbum.album_id)},
        synchronize_session=False,
    )
    ca_list = db_session3.query(CollectionAlbum).filter(
        CollectionAlbum.album_id.in_(old_uuid_list)
    )
    for ca in ca_list:
        db_session3.delete(ca)
    db_session3.commit()

    # update related_albums
    db_session4.query(RelatedAlbum).filter(RelatedAlbum.album_id.in_(uuid_dict)).update(
        {RelatedAlbum.album_id: case(uuid_dict, value=RelatedAlbum.album_id)},
        synchronize_session=False,
    )
    ra_list = db_session4.query(RelatedAlbum).filter(
        RelatedAlbum.album_id.in_(old_uuid_list)
    )
    for ra in ra_list:
        db_session4.delete(ra)
    db_session4.commit()

    db_session4.query(RelatedAlbum).filter(
        RelatedAlbum.related_album_id.in_(uuid_dict)
    ).update(
        {
            RelatedAlbum.related_album_id: case(
                uuid_dict, value=RelatedAlbum.related_album_id
            )
        },
        synchronize_session=False,
    )
    raa_list = db_session4.query(RelatedAlbum).filter(
        RelatedAlbum.related_album_id.in_(old_uuid_list)
    )
    for raa in raa_list:
        db_session4.delete(raa)

    db_session4.commit()

    # update theme_album
    db_session5.query(ThemeAlbum).filter(ThemeAlbum.album_id.in_(old_id_list)).update(
        {ThemeAlbum.valid: -94}, synchronize_session=False
    )

    db_session5.query(ThemeAlbum).filter(ThemeAlbum.album_id.in_(id_dict)).update(
        {ThemeAlbum.album_id: case(uuid_dict, value=ThemeAlbum.album_id)},
        synchronize_session=False,
    )

    db_session5.commit()

    # update urimapper

    db_session6.query(URIMapper).filter(URIMapper.entity_id.in_(uuid_dict)).update(
        {URIMapper.entity_id: case(uuid_dict, value=URIMapper.entity_id)},
        synchronize_session=False,
    )

    db_session6.commit()

    # update report autocrawler top 100
    report_info = db_session7.query(
        ReportAutoCrawlerTop100Album.id,
        ReportAutoCrawlerTop100Album.ext,
        ReportAutoCrawlerTop100Album.ext["album_uuid"].label("album_uuid"),
    ).filter(
        func.json_extract(ReportAutoCrawlerTop100Album.ext, "$.album_uuid").in_(
            old_uuid_list
        )
    )

    report_info_df = get_df_from_query(report_info)

    for row in report_info_df.index:
        ext = report_info_df.at[row, "ext"].copy()
        old_uuid = report_info_df.at[row, "album_uuid"]
        ext["album_uuid"] = uuid_dict[old_uuid]
        report_info_df.at[row, "ext"] = ext

    report_info_dict = report_info_df[["id", "ext"]].to_dict("records")

    db_session7.bulk_update_mappings(ReportAutoCrawlerTop100Album, report_info_dict)

    db_session7.commit()

    # update sg_likes

    db_session8.query(SgLikes).filter(SgLikes.entity_uuid.in_(uuid_dict)).update(
        {SgLikes.entity_uuid: case(uuid_dict, value=SgLikes.entity_uuid)},
        synchronize_session=False,
    )

    db_session8.commit()

    # update album_contributors
    db_session8.query(AlbumContributor).filter(
        AlbumContributor.album_id.in_(uuid_dict)
    ).update(
        {AlbumContributor.album_id: case(uuid_dict, value=AlbumContributor.album_id)},
        synchronize_session=False,
    )

    db_session8.commit()

    # update user_narratives
    db_session9.query(UserNarrative).filter(
        UserNarrative.entity_uuid.in_(old_uuid_list)
    ).update(
        {UserNarrative.entity_uuid: case(uuid_dict, value=UserNarrative.entity_uuid)},
        synchronize_session=False,
    )

    db_session9.commit()

    # update albums

    info = db_session10.query(Album.id, Album.info.label("album_info")).filter(
        Album.uuid.in_(old_uuid_list)
    )
    info_df = get_df_from_query(info)

    db_session10.flush()
    time.sleep(5)

    info_df_new = pd.merge(info_df, merged_df, left_on="id", right_on="albumid_old")
    info_df_ = info_df_new[["albumid_new", "album_info"]]
    info_df_.columns = ["id", "info"]
    album_info_dict = info_df_.to_dict("records")
    db_session10.bulk_update_mappings(Album, album_info_dict)
    db_session10.commit()
    time.sleep(5)

    old_uuid_album = get_df_from_query(
        db_session10.query(Album.id).filter(Album.uuid.in_(old_uuid_list))
    )
    old_uuid_album["valid"] = -94
    old_uuid_album = old_uuid_album.drop_duplicates()
    old_uuid_album_dict = old_uuid_album.to_dict("records")
    db_session10.bulk_update_mappings(Album, old_uuid_album_dict)
    db_session10.flush()
    db_session10.commit()
    time.sleep(5)

    new_uuid_album = get_df_from_query(
        db_session10.query(Album.id).filter(Album.uuid.in_(new_uuid_list))
    )
    new_uuid_album["valid"] = 1
    new_uuid_album = new_uuid_album.drop_duplicates()
    new_uuid_album_dict = new_uuid_album.to_dict("records")
    db_session10.bulk_update_mappings(Album, new_uuid_album_dict)
    db_session10.commit()
    time.sleep(5)

    old_itunes_release = get_df_from_query(
        db_session11.query(ItunesRelease.id).filter(
            ItunesRelease.album_uuid.in_(old_uuid_list)
        )
    )
    old_itunes_release["valid"] = -94
    old_itunes_release = old_itunes_release.to_dict("records")
    db_session11.bulk_update_mappings(ItunesRelease, old_itunes_release)
    db_session11.commit()

    old_artist_album = db_session11.query(Artist_album).filter(
        Artist_album.album_id.in_(old_id_list)
    )

    for row in old_artist_album:
        row.valid = -94
    db_session11.commit()

    for session in [
        db_session,
        db_session2,
        db_session3,
        db_session4,
        db_session5,
        db_session6,
        db_session7,
        db_session8,
        db_session9,
        db_session10,
        db_session11,
    ]:
        session.close()


def update_new_info(
    id_list: list, merged_df, gsheet_url: str, sheet_name: str, index_slice
):
    print("running update_new_info")
    new_uuid_list = merged_df["uuid_new"].values.tolist()
    db_session = scoped_session(
        sessionmaker(autocommit=False, autoflush=False, bind=engine)
    )
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
                SgLikes.entity_uuid.label("sg_likes.entityUUID (new)"),
                AlbumContributor.album_id.label("Albumcontributors.albumID (new)"),
                ReportAutoCrawlerTop100Album.ext["album_uuid"].label(
                    "reportautocrawler_top100albums.ext->>'$.album_uuid' (new)"
                ),
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
            .join(
                ReportAutoCrawlerTop100Album,
                func.json_unquote(ReportAutoCrawlerTop100Album.ext["album_uuid"])
                == Album.uuid,
                isouter=True,
            )
            .filter(Album.uuid.in_(new_uuid_list))
        )

        new_info_df = get_df_from_query(query_new_info).drop_duplicates(
            subset=[
                "AlbumUUID (new)",
                "Artist_album.albumID (new)",
                "Chart_album.albumID (new)",
            ]
        )
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
        ["AlbumUUID (old)"]
    ]
    sheet_df = sheet_df.iloc[index_slice]
    # sheet_df["Itune ID (new)"] = sheet_df["Itune ID (new)"].astype(int)
    print("this is sheet_df from update new info")
    print(sheet_df)
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
            "reportautocrawler_top100albums.ext->>'$.album_uuid' (new)",
            "Usernarratives.entityUUID (new)",
            "Albums.info (new)",
        ]
    ]
    set_with_dataframe(
        worksheet, df, row=index_slice[0] + 3, col=18, include_column_header=False
    )


if __name__ == "__main__":

    # input here
    gsheet_url = "https://docs.google.com/spreadsheets/d/1H0t9xq2vUesfpBQoieJP6TxHbWl8D43s5kiAZ7K3Cgc/edit#gid=978085935"
    # sheet name of allmusic input data
    sheet_allmusic = "Test_1"
    # sheet name of result check sheet
    sheet_check_result = "Test_2 (check_result)_"
    # actual script part
    df = get_ituneid(gsheet_url, sheet_allmusic)
    CHUNK_SIZE = 1000
    index_slices = sliced(range(len(df)), CHUNK_SIZE)
    for index_slice in index_slices:
        print(index_slice)
        chunk = df.iloc[index_slice]
        print_old_info(
            df=chunk,
            gsheet_url=gsheet_url,
            sheet_name=sheet_check_result,
            index_slice=index_slice,
        )
        id_list = run_crawler(chunk)
        time.sleep(600)
        query_complete_crawl = get_complete_crawl(id_list)
        get_all_crawl(chunk, id_list, sheet_check_result, gsheet_url, index_slice)
        merged_df = merge_new_old_ids(query_complete_crawl, chunk)
        update_albums(merged_df=merged_df)
        get_incomplete_crawl(gsheet_url=gsheet_url, id_list=id_list)
        update_new_info(
            id_list=id_list,
            merged_df=merged_df,
            gsheet_url=gsheet_url,
            sheet_name=sheet_check_result,
            index_slice=index_slice,
        )

