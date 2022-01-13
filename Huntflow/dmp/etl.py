import os
import uuid

import pandas as pd
import requests

from configs import configs

BASE_DIR = os.getenv('HUNTFLOW_PRJ_PATH')


def ingest_data(configs, filename):
    df = pd.read_excel(os.path.join(BASE_DIR, configs['DATA_DIR'], filename))
    return df


def validate_data(df):
    columns_format = {'Должность': {'type': str, 'null_allowed': False},
                      'ФИО': {'type': str, 'null_allowed': False},
                      'Ожидания по ЗП': {'type': int, 'null_allowed': False},
                      'Комментарий': {'null_allowed': True},
                      }
    return df, df


def transform_data_for_base(df):
    df = df.rename(columns={'Должность': 'position', 'Ожидания по ЗП': 'money'})

    df['last_name'] = df['ФИО'].apply(lambda x: x.split(' ')[0])
    df['first_name'] = df['ФИО'].apply(lambda x: x.split(' ')[1])
    df['middle_name'] = df['ФИО'].apply(lambda x: x.split(' ')[2] if len(x.split(' ')) > 2 else '')
    return df


def load_to_huntflow(configs, df, batch_id):
    huntflow_params = configs['HUNTFLOW_API']
    rows_as_dict = df.to_dict(orient='records')
    print(rows_as_dict)
    for idx, r in enumerate(rows_as_dict):
        try:
            response = requests.post(huntflow_params["URL-1"], json=r, headers=huntflow_params['HEADER'])

            if 'errors' in response.json():
                raise Exception('Error response from API')
        except Exception as e:
            print(f"Failed to import row index: {idx}")
            print(f"Row: {r}", f"Batch ID: {batch_id}")
            print(e)


def transform_data_for_vacan(dff):
    dff = dff.rename(columns={'Должность': 'position', 'Комментарий': 'comment', 'Статус': 'status'})
    dff['last_name'] = df['ФИО'].apply(lambda x: x.split(' ')[0])
    dff = dff.drop(['ФИО'], axis=1)
    dff = dff.drop(['Ожидания по ЗП'], axis=1)
    return dff


def get_vac(url_vac, configs):
    huntflow_params = configs['HUNTFLOW_API']
    response = requests.get(url=url_vac, headers=huntflow_params['HEADER'])
    vac = {}
    rows_as_dict = response.json()
    for i in rows_as_dict['items']:
        try:
            if i["position"] == 'Frontend-разработчик' or i["position"] == 'Менеджер по продажам':
                vac[i['position']] = i['id']

            if 'errors' in response.json():
                raise Exception('Error response from API')
        except Exception as e:
            print(e)
    print(vac)


def get_status(url_status, configs, dff):
    huntflow_params = configs['HUNTFLOW_API']
    response = requests.get(url=url_status, headers=huntflow_params['HEADER'])
    status = {}
    rows_as_dict = response.json()
    arr = dff.to_dict(orient='records')
    for i in rows_as_dict['items']:
        try:
            dic = [i["Статус"] for i in arr]
            if i['name'] in dic:
                status[i['name']] = i['id']
        except Exception as e:
            print(e)
    return status


def get_applicants(url, configs):
    huntflow_params = configs['HUNTFLOW_API']
    response = requests.get(url=url, headers=huntflow_params['HEADER'])
    rows_as_dict = response.json()
    apl = [str(i['id']) for i in rows_as_dict['items']]
    return apl


def load_to_vacan(configs, dff, batch_id, status, apl):
    pass
    arr = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
    huntflow_params = configs['HUNTFLOW_API']
    rows_as_dict = dff.to_dict(orient='records')
    for i, j in enumerate(rows_as_dict):
        for k in status:
            if j['status'] == k:
                j['status'] = status.get(k)
    for i, j in enumerate(rows_as_dict):
        if j['position'] == "Frontend-разработчик":
            j['vacancy'] = '2'
        else:
            j['vacancy'] = '1'
    for idx, j in enumerate(rows_as_dict):
        j['files'] = [arr[-1]]
        arr.pop(-1)
    try:
        for i, j in enumerate(rows_as_dict):
            j['url_id'] = apl[0]
            apl.pop(0)
        print(rows_as_dict)
    except:
        print("apl is empty")
    for idx, r in enumerate(rows_as_dict):
        url = "https://dev-100-api.huntflow.dev/account/2/applicants/" + r['url_id'] + "/vacancy"
        payload = {
            "vacancy": r['vacancy'],
            "status": r['status'],
            "comment": r['comment'],
            "files": r['files']
        }
        print(payload)
        try:
            response = requests.post(url, json=payload, headers=huntflow_params['HEADER'])
            if 'errors' in response.json():
                raise Exception('Error response from API')
        except Exception as e:
            print(f"Failed to import row index: {idx}")
            print(f"Row: {r}", f"Batch ID: {batch_id}")
            print(e)


def save_file(df, batch_id, filename):
    filename = f"{filename}.csv"
    df.to_csv(os.path.join(BASE_DIR, configs['ETL_OUTPUT_DIR'], batch_id, filename))


if __name__ == "__main__":
    configs = configs.load_configs()
    filename = 'Тестовая база.xlsx'
    files = ''
    batch_id = str(uuid.uuid1())

    print("Execution batch id:", batch_id)
    if not os.path.exists(os.path.join(BASE_DIR, configs['ETL_OUTPUT_DIR'], batch_id)):
        os.makedirs(os.path.join(BASE_DIR, configs['ETL_OUTPUT_DIR'], batch_id))

    print("Ingesting original data...")
    df = ingest_data(configs, 'Тестовая база.xlsx')
    save_file(df, batch_id, 'post_ingest_df')

    print("Validating data...")
    valid_rows_df, invalid_rows_df = validate_data(df)
    save_file(valid_rows_df, batch_id, 'valid_rows_post_validation_df')
    save_file(invalid_rows_df, batch_id, 'invalid_rows_post_validation_df')

    print("Transforming data for loading to Huntflow candidates base...")
    df = transform_data_for_base(valid_rows_df)
    save_file(df, batch_id, 'post_transform_df')

    print("Get vacancies...")
    url_vac = "https://dev-100-api.huntflow.dev/account/2/vacancies"
    get_vac(url_vac, configs)

    print("Get statuses")
    url_status = "https://dev-100-api.huntflow.dev/account/2/vacancy/statuses"
    status = get_status(url_status, configs, df)

    print("Transforming data for loading to Huntflow vacancy...")
    dff = transform_data_for_vacan(valid_rows_df)

    print("Get applicants")
    url_get_apl = "https://dev-100-api.huntflow.dev/account/2/applicants"
    apl = get_applicants(url_get_apl, configs)

    print("Loading data to HuntFlow candidates base...")
    load_to_huntflow(configs, df, batch_id)

    print("Loading candidate to HuntFlow vacancy...")
    load_to_vacan(configs, dff, batch_id, status, apl)
