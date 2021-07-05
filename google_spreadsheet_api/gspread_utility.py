import gspread
from gspread_pandas import Spread, Client


# gspread.auth.DEFAULT_CREDENTIALS_FILENAME = credentials_path

# gc = gspread.oauth()

# config_dir = os.path.join(BASE_DIR, "sources")

# config = conf.get_config(conf_dir=config_dir, file_name="credentials.json")


def get_worksheet(url, sheet_name):
    return Spread(spread=url, sheet=sheet_name)


def get_df_from_gsheet(url, sheet_name):
    gsheet_file = Spread(spread=url, sheet=sheet_name)
    df = gsheet_file.sheet_to_df()
    return df


if __name__ == "__main__":
    url = "https://docs.google.com/spreadsheets/d/1ZUzx1smeyIKD4PtQ-hhT1kbPSTGRdu8I8NG1uvzcWr4/edit#gid=2003688570"
    sheet_name = "Maddie_experiment"
    df = get_df_from_gsheet(url, sheet_name)
    print(df.head())
