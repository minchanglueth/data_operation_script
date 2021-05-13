from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os.path
import pickle
import pandas as pd
from core import credentials_path, token_path

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def service():
    """Shows basic usage of the Sheets API.
    Prints values from a sample spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        with open(token_path, 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    return service


def gspread_values(gsheet_id, sheet_name):
    # Call the Sheets API
    sheet = service().spreadsheets()
    result = sheet.values().get(spreadsheetId=gsheet_id,
                                range=sheet_name).execute()
    values = result.get('values', [])
    return values


def add_sheet(gsheet_id, sheet_name):
    try:
        request_body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name,
                        'tabColor': {
                            'red': 0.44,
                            'green': 0.99,
                            'blue': 0.50
                        }
                    }
                }
            }]
        }

        response = service().spreadsheets().batchUpdate(
            spreadsheetId=gsheet_id,
            body=request_body
        ).execute()

        return response
    except Exception as e:
        print(e)


def delete_sheet(gsheet_id, sheet_name):
    sheet_id = get_sheet_id_from_sheet_title(gsheet_id, sheet_name)
    try:
        request_body = {
            'requests': [{
                'deleteSheet': {

                    'sheetId': sheet_id

                }
            }
            ]
        }

        response = service().spreadsheets().batchUpdate(
            spreadsheetId=gsheet_id,
            body=request_body
        ).execute()

        return response
    except Exception as e:
        print(e)


def update_value(list_result: list, range_to_update: str, gsheet_id: str):
    '''
    sheet_name!B4:B5
    '''
    body = {
        'values': list_result  # list_result is array 2 dimensional (2D)
    }
    result = service().spreadsheets().values().update(
        spreadsheetId=gsheet_id, range=range_to_update,
        valueInputOption='RAW', body=body).execute()
    print(f"\ncomplete update data, gsheet_id: {gsheet_id}, sheet_name: {range_to_update}")


def get_df_from_speadsheet(gsheet_id: str, sheet_name: str):
    # need to optimize to read df from column_index: int = 0 (default = 0)
    data = gspread_values(gsheet_id, sheet_name)
    column = data[0]
    check_fistrow = data[1]
    x = len(column) - len(check_fistrow)
    k = [None] * x
    check_fistrow.extend(k)  # if only have column name but all data of column null =>> error
    row = data[2:]
    row.insert(0, check_fistrow)
    df = pd.DataFrame(row, columns=column).apply(lambda x: x.str.strip()).fillna(value='').astype(str)
    # df.apply(lambda x: x.str.strip()).fillna(value='').astype(str)
    return df


def get_list_of_sheet_title(gsheet_id: str):
    sheet_metadata = service().spreadsheets().get(spreadsheetId=gsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    list_of_sheet_title = []
    for i in sheets:
        list_of_sheet_title.append(i['properties']['title'])
    return list_of_sheet_title


def creat_new_sheet_and_update_data_from_df(df: object, gsheet_id: str, new_sheet_name: str):
    list_of_sheet_title = get_list_of_sheet_title(gsheet_id)
    if new_sheet_name in list_of_sheet_title:
        # Delete sheet
        delete_sheet(gsheet_id=gsheet_id, sheet_name=new_sheet_name)

        # Creat new sheet and update value
        column_name = df.columns.values.tolist()
        list_result = df.values.tolist()  # transfer data_frame to 2D list
        list_result.insert(0, column_name)

        add_sheet(gsheet_id, new_sheet_name)
        range_to_update = f"{new_sheet_name}!A1"
        update_value(list_result, range_to_update,
                     gsheet_id)  # validate_value type: object, int, category... NOT DATETIME

    else:

        column_name = df.columns.values.tolist()
        list_result = df.values.tolist()  # transfer data_frame to 2D list
        list_result.insert(0, column_name)

        add_sheet(gsheet_id, new_sheet_name)
        print(f"\ncomplete create new sheet, gsheet_id: {gsheet_id}, sheet_name: {new_sheet_name}")
        range_to_update = f"{new_sheet_name}!A1"
        update_value(list_result, range_to_update, gsheet_id)  # validate_value type: object, int, category... NOT DATETIME


def create_new_gsheet(new_gsheet_title: str):
    spreadsheet = {
        'properties': {
            'title': new_gsheet_title
        }
    }
    spreadsheet = service().spreadsheets().create(body=spreadsheet,
                                                  fields='spreadsheetId').execute()
    print('https://docs.google.com/spreadsheets/d/{0}'.format(spreadsheet.get('spreadsheetId')))
    return spreadsheet.get('spreadsheetId')


def get_sheet_id_from_sheet_title(gsheet_id: str, title: str):
    sheet_metadata = service().spreadsheets().get(spreadsheetId=gsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    for i in sheets:
        if i['properties']['title'] == title:
            sheet_id = i['properties']['sheetId']
            return sheet_id
            break


def get_gsheet_name(gsheet_id: str):
    sheet_metadata = service().spreadsheets().get(spreadsheetId=gsheet_id).execute()
    gsheet_name = sheet_metadata.get('properties').get('title')
    return gsheet_name


if __name__ == "__main__":
    #     https://docs.google.com/spreadsheets/d/1aoORoNmZoBtnY_jrBDOeLTrwLF0oeJUUDAb173BMf78/edit#gid=0
    raw_df_to_upload = {'status': ['Upload thành công 100% nhé các em ^ - ^ joy xinh qua']}
    df_to_upload = pd.DataFrame(data=raw_df_to_upload)

    new_sheet_name = 'artist image cant upload'
    gsheet_id = "1r1vD9w8Iq-qwJrnSJ5JXQB4UAu5PBuUXFkxO389OlJI"
    creat_new_sheet_and_update_data_from_df(df=df_to_upload, gsheet_id=gsheet_id,
                                            new_sheet_name=new_sheet_name)
