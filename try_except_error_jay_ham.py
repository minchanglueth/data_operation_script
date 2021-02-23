import traceback
import sentry_sdk
sentry_sdk.init(
    dsn="http://532914a0477f48cd8e5dd0452f1f52d6@v-mail.vibbidi.net/5"
)


def func():
    print("this is a func")


if __name__ == "__main__":
    x = 1/0
    print(x)

    try:
        name = "joy"
        a = [1, 2, 0, 3]
        for i in a:
            func()
            z = i + 5
            k = z * 10
            print(i)
            print(1 / i)
        x = 1 / 0
        # connect db die
        with open("joy.txt", "r") as f:
            print(f.readline())
    except ZeroDivisionError:
        print("Divide by 0")
    except FileNotFoundError:
        print("File not found")
    except:  # noqa
        print("Unknown error")
        print(traceback.format_exc())
