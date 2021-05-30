# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
import requests
import time
import os
import logging
import pandas as pd
import shutil
import warnings
from collections import Counter
def retreive_bearer_token(endpoint, usr, pwd):
    headers = {
        "accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "", "username": usr, "password": pwd}
    response = requests.post(endpoint + "/token", headers=headers, data=data)
    if response.status_code >= 400:
        raise ValueError(
            "Cannot retreive access_token. Please check credentials and try again"
        )
    res_json = response.json()
    bearer_token = res_json["access_token"]
    # Bearer token for API usage
    bearer = "Bearer " + bearer_token
    return bearer

def upload_files(doc, file_type, headers, input_handle, endpoint):
    with input_handle.get_download_stream(doc) as f:
        upload = {
            "file": (doc, f)
        }  # To include just the filename use os.path.basename(doc)
        response = requests.post(
            endpoint
            + "/documents/?document_type="
            + file_type.lower()
            + "&language=xx",
            headers=headers,
            files=upload,
        )
        return response

def check_response(response_obj):
    if response_obj.status_code == 200 or response_obj.status_code == 201:
        return True
    elif response_obj.status_code == 402:
        return "quota_exceeded"
    else:
        return False

def get_box_details(box_val):
    id = list()
    p_num = list()
    if box_val is not None:
        for b_ref in box_val:
            p_num.append(str(b_ref["page_num"]))
            id.append(str(b_ref["bbox_id"]))
    return id, p_num

def append_to_list(category, sub_category, value, doc_type, docu_name, b_id, p_num):
    global l1, l2, l3, doc, document_name, box_id, page_number
    if category != "document_type":
        l1.append(category)
        l2.append(sub_category)
        l3.append(value)
        doc.append(doc_type)
        document_name.append(docu_name)
        if isinstance(b_id, list) and isinstance(p_num, list):
            if len(b_id) == 0 and len(p_num) == 0:
                box_id.append("None")
                page_number.append("None")
            else:
                box_id.append(",".join(b_id))
                page_number.append(",".join(p_num))
        else:
            box_id.append(b_id)
            page_number.append(p_num)

def get_category_values(val, sub_cat=""):
    if val is None and sub_cat != "bbox_refs":
        append_to_list(
            category_name, sub_cat, "None", upload_type, doc_name, "None", "None"
        )
    elif isinstance(val, dict):
        if "value" in val:
            b_id, p_num = get_box_details(val["bbox_refs"])
            append_to_list(
                category_name, sub_cat, val["value"], upload_type, doc_name, b_id, p_num
            )
        else:
            for k, v in val.items():
                get_category_values(v, k)
    elif isinstance(val, list):
        if len(val) == 0:
            append_to_list(
                category_name, sub_cat, "None", upload_type, doc_name, "None", "None"
            )
        for va in val:
            get_category_values(va)
    elif isinstance(val, str):
        if val:
            append_to_list(
                category_name, sub_cat, val, upload_type, doc_name, "None", "None"
            )

def create_dataframe(extractions_response):
    global category_name
    for key, val in extractions_response.items():
        category_name = key
        get_category_values(val)

def write_to_dataset(file_info):
    global doc_name
    file_id = file_info["uuid"]
    file_name = os.path.basename(file_info["file_upload"])
    doc_name = file_name
    upload_details = requests.get(endpoint + "/documents/" + file_id, headers=headers)
    if check_response(upload_details):
        # Keep polling till the endpoint finishes processing the document
        if upload_details.json()["processing_status"] == "pending":
            time.sleep(2)
            write_to_dataset(file_info)
        elif upload_details.json()["processing_status"] == "success":
            response = requests.get(
                endpoint + "/documents/" + file_id + "/extractions",
                headers=headers,
            )
            extraction_response = response.json()
            create_dataframe(extraction_response)

def rename_duplicates():
    global l1, l2, document_name, doc
    mylist = list()
    for i in range(0, len(document_name)):
        cat_subcat = None
        if l2[i] == "":
            cat_subcat = str(l1[i])
        else:
            cat_subcat = str(l1[i]) + "/" + str(l2[i])
        tmp = str(document_name[i]) + "/" + str(doc[i]) + "/" + cat_subcat
        mylist.append(tmp)
    counts = {k: v for k, v in Counter(mylist).items() if v > 1}
    for i in reversed(range(len(mylist))):
        item = mylist[i]
        if item in counts and counts[item]:
            mylist[i] += "-" + str(counts[item])
            counts[item] -= 1
    for i in range(0, len(mylist)):
        tmp = mylist[i].split("/")
        l1[i] = tmp[2]
        if len(tmp) == 4:
            l2[i] = tmp[3]
        else:
            l2[i] = "None"

# Read recipe inputs
input_handle = dataiku.Folder("dzniC9Ih")
# Write recipe outputs
findataset = dataiku.Dataset("Output")

endpoint = "https://api.natif.ai"
allowed_filetypes = ["jpg", "jpeg", "tif", "tiff", "png", "pdf", "gif"]
#----------Lists containing the data
doc_name=str()
l1 = list()
l2 = list()
l3 = list()
doc = list()
document_name = list()
box_id = list()
page_number = list()
#----------
upload_type='Invoice'
usr='shettyvinay50@gmail.com'
pwd='Vinay123456!'
bearer = retreive_bearer_token(endpoint, usr, pwd)
headers = {
            "accept": "application/json",
            "Authorization": bearer,
        }
input_folder_paths = input_handle.list_paths_in_partition()
file_info=dict()
for file_path in input_folder_paths:
    upload_name = os.path.basename(file_path)
    if upload_name.split(".")[1] in allowed_filetypes:
        fil_upload_details = upload_files(file_path, upload_type, headers, input_handle, endpoint)
        resp_chk = check_response(fil_upload_details)
        if resp_chk == "quota_exceeded":
            raise ValueError("You have reached your document quota this month!")
        elif resp_chk:
            upload_details = fil_upload_details.json()
            file_info["uuid"] = upload_details["uuid"]
            file_info["file_upload"] = file_path
            write_to_dataset(file_info)
    else:
        warnings.warn('Cannot accept file type of file : '+ upload_name)
rename_duplicates()
extractions_dataset = {
        "Document_name": document_name,
        "Document_type": doc,
        "category": l1,
        "sub-category": l2,
        "text": l3,
        "page_number": page_number,
        "box_id": box_id,
    }
df = pd.DataFrame(
        extractions_dataset,
        columns=[
            "Document_name",
            "Document_type",
            "category",
            "sub-category",
            "text",
            "page_number",
            "box_id",
        ],
    )
#df = df.applymap(str)
if len(df) > 0:
    findataset.write_with_schema(df, dropAndCreate=True)
else:
    warnings.warn("No rows to write.")
