import pandas as pd
import requests
import datetime
import hashlib
import base64
import hmac


def csv_to_json(url, use_type=False):
    """
    convert csv format data into json format, in order to send data to Log
    Analytics.
    :param url: url from which to download the csv file
    :param use_type: when True, special types as number and timestamp
    preserve their type while inserted to the Log Analytics workspace
    :return: data in json format
    '%Y-%m-%d %H:%M:%S.%f'
    """

    for chunk in pd.read_csv(url, delimiter=',',
                             index_col=False,
                             infer_datetime_format=True,
                             chunksize=1000):
        for col in list(chunk.columns.values):
            if 'time' in col.lower():
                chunk[col] = pd.to_datetime(chunk[col],
                                            format='%Y-%m-%d %H:%M:%S.%f')
        if use_type:
            adapt_columns_name(df=chunk)
        yield chunk.to_json(orient='records', date_format='iso')


def adapt_columns_name(df):
    """
    iterate over the data's columns and add suffix to columns' names before
    pushing to Log Analytics
    :param df: pandas dataframe object
    :return:
    """
    types = df.columns.to_series().groupby(df.dtypes).groups
    for k, v in types.items():
        for c_name in v.array:
            df.rename(columns={c_name: f"{c_name}{get_suffix(k.name)}"},
                      inplace=True)


def get_suffix(pd_time):
    """
    based on the pandas data type, as suffix returned.
    String = _s
    Boolean = _b
    Int = _d
    Date/Time = _t
    Object = _s
    :param pd_time: 
    :return: suffix based on the type of pandas object
    """
    if 'int' in pd_time or 'float' in pd_time:
        suffix = '_d'
    elif 'datetime' in pd_time:
        suffix = '_t'
    elif 'boolean' in pd_time:
        suffix = '_b'
    else:
        suffix = '_s'
    return suffix


def build_signature(customer_id, shared_key, date, content_length, method,
        content_type, resource):
    """
    Build the API signature
    :param customer_id: The unique identifier for the Log Analytics workspace.
    :param shared_key: Primary key for authentication.
    :param date: data to encode in json format.
    :param content_length: length of the raw data
    :param method: the HTTP method that the client uses.
    :param content_type: http content type, i.e application/json
    :param resource: Log Analytics table name
    :return: sha256 encoded string
    """
    x_headers = f"x-ms-date:{date}"
    string_to_hash = f"{method}\n{str(content_length)}\n{content_type}\n" \
                     f"{x_headers}\n{resource}"
    bytes_to_hash = bytes(string_to_hash, encoding='utf8')
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash,
                                             digestmod=hashlib.sha256).digest())
    authorization = f"SharedKey {customer_id}:{encoded_hash.decode('utf-8')}"
    return authorization


def post_data(customer_id, shared_key, body, log_type):
    """
    Build and send a request to the POST API
    :param customer_id: The unique identifier for the Log Analytics workspace.
    :param shared_key: Primary key for authentication.
    :param body: json data
    :param log_type: Log Analytics table name
    """
    method = "POST"
    content_type = "application/json"
    resource = "/api/logs"
    rfc1123date = datetime.datetime.utcnow().strftime(
        '%a, %d %b %Y %H:%M:%S GMT')
    content_length = len(body)
    signature = build_signature(customer_id, shared_key, rfc1123date,
                                content_length, method, content_type, resource)
    uri = f"https://{customer_id}.ods.opinsights.azure.com" \
          f"{resource}?api-version=2016-04-01"

    headers = {
        'content-type': content_type,
        'Authorization': signature,
        'Log-Type': log_type,
        'x-ms-date': rfc1123date
    }
    # send HTTP request
    response = requests.post(uri, data=body, headers=headers)
    # verify HTTP response code
    if not (response.status_code >= 200 and response.status_code <= 299):
        raise Exception(f"Failed to use API. Response code: "
                        f"{response.status_code}\n Failed with message: "
                        f"{response.text}")
