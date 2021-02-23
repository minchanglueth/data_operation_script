import cv2
import time

def decode_fourcc(v: int):
    """
    avc1: H.264
    av01: av1
    https://github.com/opencv/opencv/blob/master/samples/python/video_v4l2.py
    """
    v = int(v)
    return "".join([chr((v >> 8 * i) & 0xFF) for i in range(4)])
def get_video_decode(url: str):
    import cv2
    vidcap = cv2.VideoCapture(url)
    vidcap.set(cv2.CAP_PROP_POS_MSEC, 3000)  # just cue to 20 sec. position
    fourcc = vidcap.get(cv2.CAP_PROP_FOURCC)
    decode_name = decode_fourcc(fourcc)
    return decode_name
if __name__ == "__main__":
    print("hello world")
    print(
        get_video_decode(
            "https://berserker6.vibbidi-vid.com/vibbidi-us/videos/video_606BBAD9746140BEB655082E53947753.mp4"
        )
    )