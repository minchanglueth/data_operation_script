import cv2
import time


def decode_fourcc(v: int):
    '''
    avc1: H.264
    av01: av1
    https://github.com/opencv/opencv/blob/master/samples/python/video_v4l2.py
    '''
    v = int(v)
    return "".join([chr((v >> 8 * i) & 0xFF) for i in range(4)])


def get_video_decode(url: str):
    '''
    enum cv::VideoCaptureProperties
    document: https://docs.opencv.org/3.4/d4/d15/group__videoio__flags__base.html#ggaeb8dd9c89c10a5c63c139bf7c4f5704da7c2fa550ba270713fca1405397b90ae0
    :return:
    '''

    vidcap = cv2.VideoCapture(url)
    fourcc = vidcap.get(cv2.CAP_PROP_FOURCC)
    decode_name = decode_fourcc(fourcc)
    return decode_name

def get_video_image_by_second(url: str):
    vidcap = cv2.VideoCapture(url)
    vidcap.set(cv2.CAP_PROP_POS_MSEC, 4000)  # just cue to 20 sec. position
    success,image = vidcap.read()
    if success:
        cv2.imwrite("joy_xinh.jpg", image)     # save frame as JPEG file
        # cv2.imshow('joy_xinh',image)
        cv2.waitKey()


if __name__ == "__main__":
    urls = [
        "https://berserker6.vibbidi-vid.com/vibbidi-us/videos/video_606BBAD9746140BEB655082E53947753.mp4"
    ]

    start_time = time.time()
    for url in urls:

        # get_video_image_by_second(url)

        k = get_video_decode(url)
        print(f"{k} ----{url}")

    print("\n --- total time to process %s seconds ---" % (time.time() - start_time))
    # success,image = vidcap.read()
