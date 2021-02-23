import time
import inspect
import sys


def hello():
    print(inspect.stack()[0][3])



if __name__ == "__main__":
    start_time = time.time()
    hello()

    # pd.set_option("display.max_rows", None, "display.max_columns", 80, 'display.width', 1000)
    # start_time = time.time()
    # INPUT HERE:
    # Input_url 'https://docs.google.com/spreadsheets/d/1wMLmbaY3ZJiPDU2uekzfVVLjwHOzGJ2TpYFNCWTNqAE'
    # gsheet_id = '1wMLmbaY3ZJiPDU2uekzfVVLjwHOzGJ2TpYFNCWTNqAE'  # Single page
    # sheet_name = 'ContributedAlbums_2'
    # joy = get_df_from_speadsheet(gsheet_id, sheet_name).fillna(value='None').apply(lambda x: x.str.strip())

    # PROCESS HERE:
    # intern_checking_process()


    print("--- %s seconds ---" % (time.time() - start_time))
