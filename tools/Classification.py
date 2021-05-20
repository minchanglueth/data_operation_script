
import time
import pandas as pd
import numpy as np
from scipy.stats import mode, zscore
import matplotlib.pyplot as plt
import seaborn
import re

import statsmodels.api as sm
import statsmodels.formula.api as smf
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split


def get_churn_data(path):
    churn = pd.read_excel(CHURN)
    churn.drop(columns='Phone', inplace=True)

    # Process columns name
    new_columns = []
    for i in churn.columns:
        new_name = re.sub(r'\W+', '_', i)
        if re.match('^\d', new_name):
            new_name = '_' + new_name
        new_columns.append(new_name)
    churn.columns = new_columns
    churn = pd.get_dummies(churn)
    return churn


if __name__ == "__main__":
    start_time = time.time()
    pd.set_option("display.max_rows", None, "display.max_columns", 50, 'display.width', 1000)
    CHURN = "https://github.com/pnhuy/datasets/raw/master/Churn.xls"
    df = get_churn_data(CHURN)
    lower_names = [name.lower() for name in df.columns]
    df.columns = lower_names

    # feature selection
    X = df.drop(columns=['churn'])
    Y = df.churn
    names = X.columns
    rf = RandomForestClassifier()
    rf.fit(X, Y)

    feature_importance = pd.DataFrame(
        {
            'names': names,
            'feature_importance': rf.feature_importances_
        }
    )

    sorted_fi = feature_importance.sort_values(by="feature_importance", ascending=False)
    top_feature = sorted_fi['names'].head(10).values.tolist()

    subset_churn = df[top_feature + ['churn']]
    # Chia tâpj dữ liệu thành train và test. tuy nhiên dữ liệu bị lấy lần lượt, không đảm bảo tính random => nên sử dụng thư viện để chia tâp dữ liệu
    # churn_train = subset_churn[:2222]
    # churn_test = subset_churn[2222:]

    churn_train, churn_test = train_test_split(subset_churn, test_size=0.3)
    formular = 'churn ~ ' + ' + '.join(top_feature)
    print(formular)

    # logreg = smf.logit(formular, data=churn_train).fit()

    # Loại bỏ các biến có p_value cao:
    k = 'churn ~ custserv_calls + int_l_plan + intl_calls + night_mins'
    logreg = smf.logit(k, data=churn_train).fit()
    print(logreg.summary())



    # OLS base top feature

    # df = df[top_feature + ['saleprice']]
    # ols_features = ""
    # for feature in top_feature:
    #     ols_features = ols_features + ' + ' + feature
    # ols_features = ols_features[3:]
    # print(ols_features + "\n")
    #
    # results = smf.ols(f"saleprice ~ {ols_features}", data=df).fit()
    # print(results.summary())



def process_S_11(urls: list, sheet_info: dict):
    '''
    S_11 = {"sheet_name": "S_11",
            "column_name": ["release_date", "album_title", "album_artist", "itune_album_url", "sportify_album_url"]}
    '''
    S_11_df = pd.DataFrame()
    for url in urls:
        gsheet_id = get_gsheet_id_from_url(url=url)
        sheet_name = sheet_info['sheet_name']
        original_df = get_df_from_speadsheet(gsheet_id, sheet_name)
        #     # Refactor column name before put into datalake
        original_df.columns = original_df.columns.str.replace('Release_date', 'release_date')
        original_df.columns = original_df.columns.str.replace('AlbumTitle', 'album_title')
        original_df.columns = original_df.columns.str.replace('AlbumArtist', 'album_artist')
        original_df.columns = original_df.columns.str.replace('Itunes_Album_URL', 'itune_album_url')
        original_df.columns = original_df.columns.str.replace('AlbumURL', 'sportify_album_url')
        filter_df = original_df[(original_df.itune_album_url != 'not found')].reset_index()
        info = {"url": f"{url}", "gsheet_id": f"{gsheet_id}",
                "gsheet_name": f"{get_gsheet_name(gsheet_id=gsheet_id)}",
                "sheet_name": f"{sheet_name}"}
        filter_df['gsheet_info'] = f"{info}"
        S_11_df = S_11_df.append(filter_df, ignore_index=True)
    return S_11_df