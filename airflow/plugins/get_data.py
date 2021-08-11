import json
from airflow.models import Variable

def __create_credentials():
    __user_name = Variable.get("KAGGLE_USERNAME")
    __key = Variable.get("KAGGLE_KEY")
    __kaggle_json = {"username":__user_name,"key":__key}
    with open("kaggle.json", "w") as f:
        json.dump(__kaggle_json, f)