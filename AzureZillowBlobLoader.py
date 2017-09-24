from azure.storage.blob import BlockBlobService
import pandas as pd
import time
import os


STORAGEACCOUNTNAME = 'zillowkaggle'
STORAGEACCOUNTKEY = 'c0Cwlr3X4jGl7s2x660IAnYim3OkUi5vvgFh6eIge6E0NPXsCBUJ9To3NpZsk3cKQG6xZJSv7xoTpwxu8gbzQA=='
CONTAINERNAME = 'src'
blob_service = BlockBlobService(account_name=STORAGEACCOUNTNAME,account_key=STORAGEACCOUNTKEY)

def blob_to_pandas(blob_service, container_name, blob_name, ftype = 'csv', parse_dates=None):
    LOCALFILENAME = 'tmp'
    t1=time.time()
    blob_service.get_blob_to_path(container_name,blob_name,LOCALFILENAME)
    t2=time.time()
    print((blob_name + " downloaded in %s") % (t2 - t1))
    fread = lambda f: pd.read_csv(f, parse_dates = parse_dates)
    res = fread(LOCALFILENAME)
    os.remove(LOCALFILENAME)
    return res


def load_zillow_src_data():
    df_prop = df = blob_to_pandas(blob_service, 'src', 'properties_2016.csv')
    df_train = blob_to_pandas(blob_service, 'src', 'train_2016_v2.csv', parse_dates=["transactiondate"])
    df_smpl_submission = blob_to_pandas(blob_service, 'src', 'sample_submission.csv')
    return df_prop, df_train, df_smpl_submission