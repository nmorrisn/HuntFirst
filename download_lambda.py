import os
import sys
from urllib.request import urlopen
import zipfile
from io import BytesIO

import boto3

def get_lambda_functions_code_url():

    client = boto3.client("lambda")

    lambda_functions = [n["FunctionName"] for n in client.list_functions()["Functions"]]

    functions_code_url = []

    for fn_name in lambda_functions:
        fn_code = client.get_function(FunctionName=fn_name)["Code"]
        fn_code["FunctionName"] = fn_name
        functions_code_url.append(fn_code)

    return functions_code_url


def download_lambda_function_code(fn_name, fn_code_link, dir_path):

    function_path = os.path.join(dir_path, fn_name)
    if not os.path.exists(function_path):
        os.mkdir(function_path)

    with urlopen(fn_code_link) as lambda_extract:
        with zipfile.ZipFile(BytesIO(lambda_extract.read())) as zfile:
            zfile.extractall(function_path)

functions = get_lambda_functions_code_url()

for i in range(len(functions)):
    download_lambda_function_code(functions[i]["FunctionName"], functions[i]["Location"], "/Users/nmorrisonjemio/github/huntfirst")