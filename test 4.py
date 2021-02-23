from core.crud.sql.artist import get_all_by_ids

import io
from PIL import Image
import requests
import time


class Image_size:
    micro = 80
    tiny = 160
    small = 320
    medium = 640
    large = 800
    extra = 1280


def get_artist_image_url(artist_uuids: list):
    db_artists = get_all_by_ids(artist_uuids)
    for db_artist in db_artists:
        ext = db_artist.ext['resize_images']
        with open("query_result_image_size.txt", "a") as f:
            for resize_images in ext:
                type = resize_images.split(".")[-2]
                image_url = f"https://s3.amazonaws.com/{resize_images}"
                original_image_url = f"https://s3.amazonaws.com/{db_artist.ext['square_image']['uri']}"
                if type == 'micro':
                    checking = test_2(image_url) == Image_size.micro
                elif type == 'tiny':
                    checking = test_2(image_url) == Image_size.tiny
                elif type == 'small':
                    checking = test_2(image_url) == Image_size.small
                elif type == 'medium':
                    checking = test_2(image_url) == Image_size.medium
                elif type == 'large':
                    checking = test_2(image_url) == Image_size.large
                elif type == 'extra':
                    checking = test_2(image_url) == Image_size.extra
                else:
                    print("Error")
                result = f"type: {type}, artist_uuid: {db_artist.uuid}, original_image_url: {original_image_url}, image_url: {image_url}, size: {test_2(image_url)}, result: {checking}"
                print(result)
                f.write(result + "\n")


def test_2(uri):
    response = requests.get(uri)
    image_bytes = io.BytesIO(response.content)
    img = Image.open(image_bytes)
    return img.size


if __name__ == "__main__":
    start_time = time.time()
    uri = "https://aimg.vibbidi-vid.com/vibbidi-images/artists/img_E560F23B0B074F1DB2996070C11CA43D.2019.09.05.14.51.47.medium.jpg"
    k = test_2(uri)
    print(k)
    # artist_uuids = [
    #     "71BBDC06F6F34983853B3EAF5491EACA",
    #     "D96658C02F8C4293A16AE37CE2CBC387",
    #     "1422E730478D4EFFB637B0319A50F2FD"
    # ]
    # get_artist_image_url(artist_uuids)

    print("--- %s seconds ---" % (time.time() - start_time))
