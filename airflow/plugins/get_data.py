import json
from airflow.models import Variable
from opendatasets.utils.kaggle_api import download_kaggle_dataset

def __create_credentials():
    __user_name = Variable.get("KAGGLE_USERNAME")
    __key = Variable.get("KAGGLE_KEY")
    __kaggle_json = {"username":__user_name,"key":__key}
    with open("kaggle.json", "w") as f:
        json.dump(__kaggle_json, f)

        
def get():
    __create_credentials()
    download_kaggle_dataset("https://www.kaggle.com/ealaxi/banksim1?select=bs140513_032310.csv", data_dir='./tmp', force=True)