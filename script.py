import pandas as pd
import numpy as np
import math
import requests
import json
import glob
import sys
import os
from io import (StringIO, BytesIO)
import time
import datetime
import pytz
from joblib import (Parallel, delayed)
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from shareplum import (Site, Office365)
from shareplum.site import Version

# Init script execution time for logging purposes
start_time = datetime.datetime.now().astimezone(pytz.timezone("Australia/Sydney"))

# Limit Intel MKL and OpenMP to single-threaded execution to prevent thread oversubscription
# Oversubscription can cause code execution for parallelization to be stuck
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_DYNAMIC"] = "FALSE"

# Init credential secrets from Azure Key Vault
vault_name = ""
url_vault = f"https://{vault_name}.vault.azure.net/"
client_vault = SecretClient(vault_url = url_vault, credential = DefaultAzureCredential())
api_token_ActiveCampaign = client_vault.get_secret("API_TOKEN_ActiveCampaign").value
username_sharepoint = client_vault.get_secret("username_SharePoint").value
password_sharepoint = client_vault.get_secret("password_SharePoint").value

# Init SharePoint path dirs to process the relevant files 
url_sharepoint = "https://heartresearchinstitute.sharepoint.com"
site_sharepoint_from_JO = ""
site_sharepoint_from_NO = ""
site_sharepoint_log_dump = ""
path_sharepoint_folder_from_JO = ""
path_sharepoint_folder_from_NO = ""
path_sharepoint_folder_log_dump = ""
auth_sharepoint = Office365(url_sharepoint, username = username_sharepoint, password = password_sharepoint) \
                  .GetCookies()
folder_sharepoint_from_JO = Site(f"{url_sharepoint}/{site_sharepoint_from_JO}", version = Version.v365, 
                                 authcookie = auth_sharepoint) \
                            .Folder(path_sharepoint_folder_from_JO)
folder_sharepoint_from_NO = Site(f"{url_sharepoint}/{site_sharepoint_from_NO}", version = Version.v365, 
                                 authcookie = auth_sharepoint) \
                            .Folder(path_sharepoint_folder_from_NO)
folder_sharepoint_log_dump = Site(f"{url_sharepoint}/{site_sharepoint_log_dump}", version = Version.v365, 
                                  authcookie = auth_sharepoint) \
                             .Folder(path_sharepoint_folder_log_dump)
files_sharepoint_from_JO = folder_sharepoint_from_JO.files
files_sharepoint_from_NO = folder_sharepoint_from_NO.files

# Init vars for collating unsubbed & bounced contacts
url_bulk_import_contact = "https://hri618.api-us1.com/api/3/import/bulk_import"
headers_post_bulk_import_contact = {
    "accept": "application/json",
    "Content-Type": "application/json",
    "Api-Token": api_token_ActiveCampaign
}
current_date = datetime.datetime.now().date()
year, month, day = current_date.year, current_date.month, current_date.day
weekly_multiplier, lookback_days = 1, 7
start_date = datetime.date(year, month, day) \
             - datetime.timedelta(days = current_date.weekday()) \
             - datetime.timedelta(days = lookback_days) \
             - datetime.timedelta(days = 1)
end_date = start_date \
           + datetime.timedelta(days = (7 * weekly_multiplier) + 1)
url_bounced_contact = "https://hri618.api-us1.com/api/3/contacts?status=3"
url_unsubbed_contact = "https://hri618.api-us1.com/api/3/contacts?status=2"
headers_get_bounced_unsubbed_contact = {
    "accept": "application/json",
    "Api-Token": api_token_ActiveCampaign
}

# Helper function to parse payload to be at most about 90% of max allowable size of 
# <= 400k B for each bulk import POST request
# Max allowable size is per ActiveCampaign's docs & 90% limit is arbitrarily chosen 
# as buffer for guaranteed safe POST request
def payload_parser(payload):
    list_index = [0]
    len_payload = len(payload["contacts"])
    payload_size_bytes = sys.getsizeof(json.dumps(payload["contacts"]))
    while list_index[-1] < len_payload:
        last_index = list_index[-1] + int(360000 / payload_size_bytes * len_payload) + 1
        list_index.append(last_index)
    list_index[-1] = len_payload 
        
    return list_index

# Helper function to get all bounced contacts
# Has to get all as there's no functionality to filter based on bounced date range
def get_bounced_contacts(index):
    _response = requests.get(url_bounced_contact, headers = headers, 
                             params = {"limit": 100, "offset": 100 * index})
    _response = [
        {k: index[k] for k in ["email", "firstName", "lastName", "bounced_date", "id"] if k in index} \
        for index in _response.json()["contacts"]
    ]
    time.sleep(1) # Limiter to accomodate ActiveCampaign's API policy of max 5 requests per second 
    
    return _response

# Helper function to get all unsubbed contacts
# Has to get all as there's no functionality to filter based on unsubbed date range
def get_unsubbed_contacts(index):
    _response = requests.get(url_unsubbed_contact, headers = headers, 
                             params = {"limit": 100, "offset": 100 * index})
    _response = [
        {k: index[k] for k in ["email", "firstName", "lastName", "cdate", "udate", "id"] if k in index} \
        for index in _response.json()["contacts"]
    ]
    time.sleep(1) # Limiter to accomodate ActiveCampaign's API policy of max 5 requests per second 
    
    return _response

# Helper function to parallelize getting all bounced or unsubbed contacts
def process_contacts(iterator, kind):
    list_response = []
    if kind == "bounced":
        _response = Parallel(n_jobs = 5, verbose = 0)(
            delayed(get_bounced_contacts)(i) for i in iterator
            )
        list_response.extend(_response)
    if kind == "unsubbed":
        _response = Parallel(n_jobs = 5, verbose = 0)(
            delayed(get_unsubbed_contacts)(i) for i in iterator
            )
        list_response.extend(_response)
    
    return list_response

# Helper function to get `RE - Constituent ID` for each bounced or unsubbed contact
# `RE - Constituent ID` is a custom field that corresponds to HRI's internal CRM contact ID 
def get_contacts_cons_id(cons_id):
    _url = f"https://hri618.api-us1.com/api/3/contacts/{cons_id}"
    _response = requests.get(_url, headers = headers_get_bounced_unsubbed_contact)
    _cons_id = [np.nan if len([i for i in _response.json()["fieldValues"] if i["field"] == "2"]) == 0 \
                else [i for i in _response.json()["fieldValues"] if i["field"] == "2"][0]["value"]][0]
    time.sleep(1) # Limiter to accomodate ActiveCampaign's API policy of max 5 requests per second 
    
    return _cons_id

# Helper function to parallelize getting all `RE - Constituent ID` for each bounced or unsubbed contact
def process_cons_id(iterator):
    list_cons_id = []
    _cons_id = Parallel(n_jobs = 5, verbose = 0)(
        delayed(get_contacts_cons_id)(i) for i in iterator
        )
    list_cons_id.extend(_cons_id)
    
    return list_cons_id

# Process welcome mailing list files
to_import_from_JO = list()
for i in files_sharepoint_from_JO:
    if (i["Name"].endswith(".xlsx")) or (i["Name"].endswith(".xls")):
        df = pd.read_excel(BytesIO(folder_sharepoint_from_JO.get_file(i).decode("utf-8")))
    if i["Name"].endswith(".csv"):
        df = pd.read_csv(StringIO(folder_sharepoint_from_JO.get_file(i).decode("utf-8")))
    df["DOB"] = df["DOB"].astype("str")
    df["1stDebitDate"] = df["1stDebitDate"].astype("str")
    df["listid"] = np.select([
                                 "Welcome" in i.split(".")[0] # Donor Series - Welcome - Australia
                                 "1stWeekEmail" in i.split(".")[0] # Donor Series - Australia
                                 "2ndMonthEmail" in i.split(".")[0] # Donor Series - Month 2 - Australia   
                                 "3rdMonthEmail" in i.split(".")[0] # Donor Series - Month 3 - Australia
                                 "2YearEmail" in i.split(".")[0] # Donor Series - 2 Years - Australia    
                             ],
                             [
                                 "71",
                                 "26",
                                 "246",
                                 "241",           
                                 "72"
                             ])
    dict_df = df.to_dict()
    for j in range(len(df)):
        to_import_from_JO.append(
            {
                "email": dict_df["Email"][j],
                "first_name": dict_df["FirstName"][j],
                "last_name": dict_df["Surname"][j],
                "phone": dict_df["Mobile"][j],
                "tags": [
                    i.split("\\")[-1].split(".")[0]
                ],
                "fields": [                
                    {"id": 2, "value": dict_df["SerialNum"][j]}, # RE - Constituent ID
                    {"id": 5, "value": dict_df["Title"][j]}, # Title
                    {"id": 24, "value": dict_df["Address"][j]}, # Address
                    {"id": 25, "value": dict_df["Suburb"][j]}, # Suburb
                    {"id": 26, "value": dict_df["State"][j]}, # State
                    {"id": 27, "value": dict_df["Postcode"][j]}, # Postcode
                    {"id": 28, "value": dict_df["DOB"][j]}, # DOB
                    {"id": 29, "value": dict_df["1stDebitDate"][j]}, #1stDebitDate
                    {"id": 30, "value": dict_df["Amount"][j]} # Amount
                ],
                "subscribe": [
                    {"listid": df["listid"][j]} # ActiveCampaign List to subscribe to
                ]
            }
        )
payload = {
    "contacts": to_import_from_JO
}
payload_index = payload_parser(payload)
for i in range(len(payload_index) - 1):
    _payload = {
        "contacts": to_import_from_JO[payload_index[i] : payload_index[i + 1]]
    }  
    requests.post(url, 
                  json = _payload, 
                  headers = headers_post_bulk_import_contact)
    time.sleep(1) # Limiter to accomodate ActiveCampaign's API policy of max 5 requests per second 

# Process contacts for segementation
to_import_from_NO = list()
for i in files_sharepoint_from_NO:
    if (i["Name"].endswith(".xlsx")) or (i["Name"].endswith(".xls")):
        df = pd.read_excel(BytesIO(folder_sharepoint_from_NO.get_file(i).decode("utf-8")))
    if i["Name"].endswith(".csv"):
        df = pd.read_csv(StringIO(folder_sharepoint_from_NO.get_file(i).decode("utf-8")))
    df["Constituent Number"] = df["Constituent Number"].astype("str")
    df["listid"] = np.select([
                                 df["Appeal"].str.contains("AU") & df["Package"].str.contains("Active"), # RG Active
                                 df["Appeal"].str.contains("AU") & df["Package"].str.contains("Lapsed"), # RG Lapsed
                                 df["Appeal"].str.contains("AU") & df["Package"].str.contains("Insight"), # SG Insight
                                 df["Appeal"].str.contains("AU") & df["Package"].str.contains("NonInsight"), # SG NonInsight
                                 # AU Philantrophy - currently not in use but still listed just in case 
                                 df["Appeal"].str.contains("NZ") & df["Package"].str.contains("Active"), # NZ RG Active
                                 df["Appeal"].str.contains("NZ") & df["Package"].str.contains("Lapsed"), # NZ RG Lapsed
                                 df["Appeal"].str.contains("NZ") & df["Package"].str.contains("Other"), # Newsletter NZ
                             ],
                             [
                                 "199",
                                 "200",
                                 "236",
                                 "237",
                                 # "229" - currently not in use but still listed just in case
                                 "256",
                                 "254",
                                 "258"
                             ])
    dict_df = df.to_dict()
    for j in range(len(df)):
        to_import_from_NO.append(
            {
                "email": dict_df["Email Address"][j],
                "first_name": dict_df["First name"][j],
                "last_name": dict_df["Last name"][j],
                "tags": [
                    i.split(".")[0] + "_" + df["Package"][j].split("_")[-1].split("-")[0]
                ],
                "fields": [                
                    {"id": 2, "value": dict_df["Constituent Number"][j]}, # RE - Constituent ID
                    {"id": 5, "value": dict_df["Title"][j]}, # Title
                    {"id": 96, "value": dict_df["Appeal"][j]}, # Appeal ID
                    {"id": 97, "value": dict_df["Package"][j]}, # Package ID
                    {"id": 134, "value": dict_df["Description"][j]}, # Description
                    {"id": 113, "value": dict_df["Informal Salutation"][j]}, # Informal Salutation
                    {"id": 46, "value": dict_df["Fullname"][j]} # First & Last Name
                ],
                "subscribe": [
                    {"listid": df["listid"][j]} # ActiveCampaign List to subscribe to
                ]
            }
        )
payload = {
    "contacts": to_import_from_NO
}
payload_index = payload_parser(payload)
for i in range(len(payload_index) - 1):
    _payload = {
        "contacts": to_import_from_NO[payload_index[i] : payload_index[i + 1]]
    }  
    requests.post(url, 
                  json = _payload, 
                  headers = headers_post_bulk_import_contact)
    time.sleep(1) # Limiter to accomodate ActiveCampaign's API policy of max 5 requests per second 

# Collate bounced contacts
response = requests.get(url_bounced_contact, headers = headers_get_bounced_unsubbed_contact, 
                        params = {"limit": 1})
iterator = range(math.ceil(int(response.json()["meta"]["total"]) / 100))
list_response = process_contacts(iterator, "bounced")
df_contacts_bounced = pd.DataFrame([i for j in list_response for i in j])
df_contacts_bounced["bounced_date"] = pd.to_datetime(df_contacts_bounced["bounced_date"]).dt.date
df_contacts_bounced = df_contacts_bounced[(df_contacts_bounced["bounced_date"] > start_date) \
                                          & (df_contacts_bounced["bounced_date"] < end_date)]
df_contacts_bounced["RE - Constituent ID"] = process_cons_id(df_contacts_bounced["id"])
df_contacts_bounced = df_contacts_bounced.drop(labels = "id", axis = 1).rename(columns = {"email": "Email", 
                                                                                          "firstName": "First Name",
                                                                                          "lastName": "Last Name",
                                                                                          "bounced_date": "Bounced Date"})
df_contacts_bounced.to_csv(".csv", index = False)

# Limiter to accomodate ActiveCampaign's API policy of max 5 requests per second
# Overshot to ensure enough time gap
time.sleep(3)

# Collate unsubbed contacts
response = requests.get(url_unsubbed_contact, headers = headers_get_bounced_unsubbed_contact, 
                        params = {"limit": 1})
iterator = range(math.ceil(int(response.json()["meta"]["total"]) / 100))
list_response = process_contacts(iterator, "unsubbed")
df_contacts_unsubbed = pd.DataFrame([i for j in list_response for i in j])
df_contacts_unsubbed["cdate"] = pd.to_datetime(df_contacts_unsubbed["cdate"].str.split("T").str[0]).dt.date
df_contacts_unsubbed["udate"] = pd.to_datetime(df_contacts_unsubbed["udate"].str.split("T").str[0]).dt.date
df_contacts_unsubbed = df_contacts_unsubbed[(df_contacts_unsubbed["udate"] > start_date) \
                                          & (df_contacts_unsubbed["udate"] < end_date)]
df_contacts_unsubbed["RE - Constituent ID"] = process_cons_id(df_contacts_unsubbed["id"])
df_contacts_unsubbed = df_contacts_unsubbed.drop(labels = "id", axis = 1).rename(columns = {"email": "Email", 
                                                                                            "firstName": "First Name",
                                                                                            "lastName": "Last Name",
                                                                                            "cdate": "Subscribed Date",
                                                                                            "udate": "Unsubscribed Date"})
df_contacts_unsubbed.to_csv(".csv", index = False)

# Log script execution metadata & upload to Fundraising SharePoint under ActiveCampaign Automation folder
df_logs = pd.DataFrame(
    {
        "executed_at_AEST": [start_time.strftime("%Y-%m-%d %H:%M:%S.%f %Z%z")],
        "duration_in_mins": [round((datetime.datetime.now() \
                                    .astimezone(pytz.timezone("Australia/Sydney")) \
                                    - start_time).total_seconds() / 60, 1)],
        "num_contacts_welcome": [len(to_import_from_JO)],
        "num_contacts_all_segments": [len(to_import_from_NO)],
        "date_range_bounced_unsubbed_contacts": [f"{start_date}_{end_date}"],
        "num_contacts_bounced": [len(df_contacts_bounced)],
        "num_contacts_unsubbed": [len(df_contacts_unsubbed)]
    }
)
# If log file exists, append newest log data & update
if any(i["Name"] == "runtime_logs.csv" for i in folder_sharepoint_log_dump.files()):
    _df_logs = pd.read_csv(StringIO(folder_sharepoint_log_dump.get_file("runtime_logs.csv").decode("utf-8")))
    _df_logs.append(df_logs, ignore_index = True)
    buffer = StringIO()
    _df_logs.to_csv(buffer, index = False, header = True)
    df_logs_to_upload = buffer.getvalue()
    folder_sharepoint_log_dump.upload_file(df_logs_to_upload, "runtime_logs.csv")
# Otherwise create a new log file & upload for future use
else:
    buffer = StringIO()
    df_logs.to_csv(buffer, index = False, header = True)
    df_logs_to_upload = buffer.getvalue()
    folder_sharepoint_log_dump.upload_file(df_logs_to_upload, "runtime_logs.csv")