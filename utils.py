import requests
import logging
from oauth_flask.config import CLIENT_ID, CLIENT_SECRET
from oauth_flask.sqlite_db import SQLiteDB
from requests.exceptions import JSONDecodeError
from gspread.exceptions import APIError
from time import sleep
import gspread
from oauth_flask.keys import GoHighLevelConfig, GoogConfig, ClickUpConfig

import sys, os

current_dir = os.path.dirname(os.path.abspath(__file__))

# Navigate up two folders
parent_dir = os.path.dirname(os.path.dirname(current_dir))
print(parent_dir)
sys.path.append(parent_dir)

from clickup_python_sdk.api import ClickupClient
from clickup_python_sdk.clickupobjects.list import List

logging.basicConfig(filename="error.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CLICKUP_CLIENT = ClickupClient.init(ClickUpConfig.ACCESS_TOKEN)


class RefreshTokenError(Exception):
    pass


DB = SQLiteDB()


def verify_response(response):
    if "error" in response:
        print(response)
        description = response["error_description"]
        raise RefreshTokenError(description)

    return True


def refresh_tokens():
    """refreshes all of the tokens in the api_data table"""
    data = DB.fetch_all_records("api_data")

    for row in data:
        try:
            refresh_token = row[6]
            refresh_one_token(refresh_token)
        # account for an empty response being sent back or an invalid refresh token
        except (JSONDecodeError, RefreshTokenError) as e:
            print(f"Error:\n  Location ID: {row[2]}\n Error: {e}")
    return True


def refresh_one_token(refresh_token):
    app_config = {"clientId": CLIENT_ID, "clientSecret": CLIENT_SECRET}

    data = {
        "client_id": app_config["clientId"],
        "client_secret": app_config["clientSecret"],
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "user_type": "Location",
        "redirect_uri": "http://localhost:3000/oauth/callback",
    }

    headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post("https://services.leadconnectorhq.com/oauth/token", data=data, headers=headers)

    if verify_response(response.json()):
        DB.insert_or_update_token(response.json())
        return True

    return False


def insert_sheets_retailer_data(mds_data):
    """creates a list of tuples to insert into the rgm_retailers table"""

    headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(mds_data[0])}

    values_to_insert = []
    for row in mds_data[1:]:
        location_id = row[headers_mapping["ghl location id"]]
        lds_link = row[headers_mapping["lead data sheet link"]]
        # active only
        status = row[headers_mapping["status"]]
        if lds_link == "" or location_id == "" or status == "Churned":
            continue
        values_to_insert.append((location_id, lds_link))

    DB.insert_many_retailer_records(
        values_to_insert,
    )

    return True


def insert_all_contacts_into_db(location_id, api_key, limit=20):
    """
    from oauth_flask.utils import insert_all_contacts_into_db
    from oauth_flask.sqlite_db import SQLiteDB

    DB = SQLiteDB()

    "Restore Hyper Wellness (Greenville)"
    location_id = "mnpHSVqel2ytv5VHQl7c"
    access_token = DB.fetch_single_record("api_data", "locationId", location_id)[3]

    contacts = insert_all_contacts_into_db(location_id, access_token)
    """

    base_url = "https://services.leadconnectorhq.com"
    endpoint = "/contacts/"
    headers = {"Authorization": f"Bearer {api_key}", "Version": "2021-07-28"}

    all_contacts = []
    next_page_url = f"{base_url}{endpoint}?locationId={location_id}&limit={limit}"

    while next_page_url:
        response = requests.get(next_page_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            contacts = data.get("contacts", [])
            all_contacts.extend(contacts)

            meta = data.get("meta", {})
            next_page_url = meta.get("nextPageUrl")
        else:
            print(f"Error: {response.text}")
            break

    DB.insert_many_contacts(
        all_contacts,
    )
    print(f"Inserted {len(all_contacts)} contacts into the database for location {location_id}")
    return all_contacts


def update_contacts_for_retailers():
    # iterate through each row of the rgm_retailers table
    retailers = DB.fetch_all_records("rgm_retailers")
    for row in retailers:
        # 1. Get the locationId and lead data sheet link from the rgm_retailer table and api key from the api_data table
        location_id = row[0]
        print(f"Querying for {location_id}")
        api_query = DB.fetch_single_record("api_data", "locationId", location_id)
        if not api_query:
            continue
        api_key = api_query[3]

        # 2. Pass in locationId and api key to the insert_all_contacts_into_db function
        insert_all_contacts_into_db(location_id, api_key, limit=100)
    return True


def update_retailers_lead_data_sheets(google_client):
    # iterate through each row of the rgm_retailers table
    retailers = DB.fetch_all_records("rgm_retailers")
    for row in retailers:
        # 1. get the lds_link from the rgm_retailers table
        location_id = row[0]
        lds_link = row[1]
        updated = False if row[2] == 0 else True

        # if already updated, skip
        if updated:
            continue

        # 2. open the lead data sheet
        lead_data_sheet = open_lds(google_client, lds_link, location_id)

        if not lead_data_sheet:
            continue

        worksheet_values = lead_data_sheet.get_all_values()

        # map the headers
        headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(worksheet_values[0])}

        # ensure the proper headers are present
        missing_headers = verify_headers(
            ["phone", "email", "first name", "last name", "contact id", "location id"], worksheet_values
        )
        if missing_headers:
            # print and write out the list of missing headers from the missing_headers list of strings
            print(f"Missing headers in location {location_id}, sheet {lds_link}, headers: {missing_headers}")
            DB.retailer_updated(location_id, 2)
            continue

        contact_id_batch, location_id_batch = create_batch(location_id, worksheet_values, headers_mapping)

        update_location_contact_ids(location_id_batch, contact_id_batch, lead_data_sheet, location_id)
        DB.retailer_updated(location_id, 1)
    return True


def verify_headers(required_headers, worksheet_values):
    """
    required_headers: list of strings that should be in the headers
    """
    headers = [header.lower().rstrip() for header in worksheet_values[0]]
    missing = []
    for header in required_headers:
        if header not in headers:
            missing.append(header)
    return missing


def create_batch(location_id, worksheet_values, headers_mapping):
    """
    Use: the function takes in an unstructured list of lists and returns a list of lists with the necessary information to correlate contacts to the correct row in the lead data sheet
    """
    # iterate through every row and attempt to correlate a contact to the row
    contact_id_batch = []
    location_id_batch = []
    for row in worksheet_values[1:]:
        # attempt to find records in the "rgm_contacts" table with matching phone numbers or emails, then try first and last name

        phone_number = format_phone_number(row[headers_mapping["phone"]])
        email = row[headers_mapping["email"]].lower() if row[headers_mapping["email"]] else None
        first_name = (
            row[headers_mapping["first name"]].lower().rstrip() if row[headers_mapping["first name"]] else None
        )
        last_name = row[headers_mapping["last name"]].lower().rstrip() if row[headers_mapping["last name"]] else None

        previous_contact_record = row[headers_mapping["contact id"]]
        previous_location_record = row[headers_mapping["location id"]]

        contact_record = DB.attempt_contact_retrieval(phone_number, email, first_name, last_name, location_id)

        # check if there is already a contact id in the row
        if previous_contact_record and previous_location_record:
            contact_id_batch.append(previous_contact_record)
            location_id_batch.append(previous_location_record)
            continue

        # if there is a contact record, append the contact id and location id to the batch
        if contact_record:
            query_contact_id = contact_record[0]
            query_location_id = contact_record[1]
            contact_id_batch.append(query_contact_id)
            location_id_batch.append(query_location_id)
        # if there is no contact record, append None to the batch
        else:
            contact_id_batch.append("")
            location_id_batch.append("")

    return contact_id_batch, location_id_batch


def format_phone_number(phone_number):
    """
    Convert phone numbers with the following format "(910) 733-9541" and "18647878082" to "+19107339541"
    """
    if phone_number == "":
        return ""
    if phone_number[0] == "(":
        return "+1" + "".join([c for c in phone_number if c.isdigit()])
    if len(phone_number) == 11:
        return "+" + phone_number
    return None


def update_location_contact_ids(location_id_batch, contact_id_batch, lds_sheet, location_id):
    """
    Use: Take in a list of contact ids and a list of location ids and updates the columns in the lead data sheet with the correct contact ids
    """

    # map the headers
    headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(lds_sheet.get_all_values()[0])}

    # determine which column contact and location ids are in
    contact_id_column = headers_mapping["contact id"]
    location_id_column = headers_mapping["location id"]

    # generate the ranges for contact_id_column and location_id_column
    contact_id_range = f"{chr(65 + contact_id_column)}2:{chr(65 + contact_id_column)}{len(contact_id_batch) + 1}"
    location_id_range = f"{chr(65 + location_id_column)}2:{chr(65 + location_id_column)}{len(location_id_batch) + 1}"

    # update the contact id column
    try:
        lds_sheet.batch_update(
            [
                {
                    "range": contact_id_range,
                    "values": [[contact_id] for contact_id in contact_id_batch],
                },
                {
                    "range": location_id_range,
                    "values": [[location_id] for location_id in location_id_batch],
                },
            ]
        )
    except APIError as e:
        code = e.args[0]["code"]
        status = e.args[0]["status"]
        if code == 429 and status == "RESOURCE_EXHAUSTED":
            print("API Error: RESOURCE_EXHAUSTED sleeping for 100 seconds")
            sleep(100)
        else:
            print(f"Error: {e} Location ID: {location_id}")
            return True
    print(f"Location {location_id} updated")
    return True


def open_lds(google_client, lds_link, location_id):
    try:
        lead_data_sheet = google_client.open_by_url(lds_link).get_worksheet(index=0)
        worksheet_values = lead_data_sheet.get_all_values()
    except APIError as e:
        code = e.args[0]["code"]
        status = e.args[0]["status"]
        if code == 429 and status == "RESOURCE_EXHAUSTED":
            print("API Error: RESOURCE_EXHAUSTED sleeping for 100 seconds")
            sleep(100)
            return open_lds(google_client, lds_link, location_id)
        elif code == 403 and status == "PERMISSION_DENIED":
            return False
        else:
            print(f"Error: {e}      Location ID: {location_id}")
            return False
    return lead_data_sheet, worksheet_values


def write_missing_contact_location_id(google_client):
    """
    Runs though the lds_links for every retailer from the rgm_table and generates a list of rows that are missing contact and location IDs.
    Creates a file named "missing_contacts.txt" and writes the results to that file.
    Formats the txt as follows
    Location ID: {locationId}, LDS Link: {lds_link}
        Row: {row}, Contact First Name: {first_name}, Contact Last Name: {last_name}
    """
    retailers = DB.fetch_all_records("rgm_retailers")
    for row in retailers:
        total_missing = ""
        lds_sheet, worksheet_values = open_lds(google_client, row[1], row[0])

        if not lds_sheet:
            continue

        missing_contacts = determine_missing_contacts(worksheet_values)

        total_missing += f"Location ID: {row[0]}, LDS Link: {row[1]}\n"
        contacts_missing = ""
        for contact in missing_contacts:
            contacts_missing += (
                f"  Row: {contact[0]}, Contact First Name: {contact[1]}, Contact Last Name: {contact[2]}\n"
            )

        if not contacts_missing:
            continue
        print(f"Location {row[0]} written")
    return True


def determine_missing_contacts(worksheet_values):
    """
    Runs through worksheet values and returns a list of lists of missing contacts
    Returns list of lists with the following structure
    [[row, first_name, last_name], [row, first_name, last_name]]
    """
    # create a mapping of the headers
    headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(worksheet_values[0])}

    for index, row in enumerate(worksheet_values[1:], start=1):
        # if the row doesn't have a value for "phone", "email", "first name", or "last name", skip
        empty = True
        for headers in ["phone", "email", "first name", "last name"]:
            if row[headers_mapping[headers]] != "":
                empty = False

        if empty:
            continue

        contact_id = row[headers_mapping["contact id"]]
        location_id = row[headers_mapping["location id"]]
        first_name = row[headers_mapping["first name"]] if row[headers_mapping["first name"]] else ""
        last_name = row[headers_mapping["last name"]] if row[headers_mapping["last name"]] else ""

        # if test in the name or last name or the first and last name is "john" and "smith", skip
        if "test" in first_name.lower() or "test" in last_name.lower():
            continue
        if first_name.lower() == "john" and last_name.lower() == "smith":
            continue
        if not contact_id and not location_id:
            yield [index, first_name, last_name]
    return None


def count_missing_contact_location_id(google_client):
    """
    Runs though the lds_links for every retailer from the rgm_table and generates a list of rows that are missing contact and location IDs.
    Creates a file named "missing_contacts.txt" and writes the results to that file.
    Formats the txt as follows
    Location ID: {locationId}, LDS Link: {lds_link}
        Row: {row}, Contact First Name: {first_name}, Contact Last Name: {last_name}
    """
    retailers = DB.fetch_all_records("rgm_retailers")
    for row in retailers:
        lds_sheet, worksheet_values = open_lds(google_client, row[1], row[0])

        if not lds_sheet:
            continue

        total_missing = f"Location ID: {row[0]}, LDS Link: {row[1]}\n"
        contact_count = count_missing_contacts(worksheet_values)

        if contact_count == 0:
            continue
        if contact_count > 20:
            print(f"Location {row[0]} written")
        else:
            print(f"Location {row[0]} written")
    return True


def count_missing_contacts(worksheet_values):
    """
    Runs through worksheet values and returns a list of lists of missing contacts
    Returns list of lists with the following structure
    [[row, first_name, last_name], [row, first_name, last_name]]
    """
    # create a mapping of the headers
    headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(worksheet_values[0])}
    count = 0
    for index, row in enumerate(worksheet_values[1:], start=1):
        # if the row doesn't have a value for "phone", "email", "first name", or "last name", skip
        empty = True
        for headers in ["phone", "email", "first name", "last name"]:
            if row[headers_mapping[headers]] != "":
                empty = False

        if empty:
            continue

        contact_id = row[headers_mapping["contact id"]]
        location_id = row[headers_mapping["location id"]]
        first_name = row[headers_mapping["first name"]] if row[headers_mapping["first name"]] else ""
        last_name = row[headers_mapping["last name"]] if row[headers_mapping["last name"]] else ""

        # if test in the name or last name or the first and last name is "john" and "smith", skip
        if "test" in first_name.lower() or "test" in last_name.lower():
            continue
        if first_name.lower() == "john" and last_name.lower() == "smith":
            continue
        if not contact_id and not location_id:
            count += 1
    return count


import requests


def get_opportunities(access_token, pipeline_id):
    headers = {"Authorization": "Bearer {}".format(access_token)}
    base_url = f"https://rest.gohighlevel.com/v1/pipelines/{pipeline_id}/opportunities?limit=100"
    opportunities = []

    while base_url:
        response = requests.get(base_url, headers=headers)

        if response.status_code == 200:
            data = response.json()
            opportunities.extend(data.get("opportunities", []))
            next_page_url = data.get("meta", {}).get("nextPageUrl", None)
            base_url = next_page_url
        else:
            print(response)
            raise Exception("Failed to fetch opportunities. Status code: {}".format(response.status_code))

    return opportunities


def get_location_pipelines_from_ghl(access_token):
    """
    Uses the first version of the gohighlevel api to get pipelines
    """

    url = "https://rest.gohighlevel.com/v1/pipelines/"

    payload = {}
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()["pipelines"]


def write_opportunity_data_to_sheets(lds_sheet, opportunities):
    """
    Batch updates a google sheet to update the opportunity data
    """
    lds_values = lds_sheet.get_all_values()
    headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(lds_values[0])}

    batch_update = []
    for row in lds_values[1:]:
        contact_id = row[headers_mapping.get("contact id", "")]  # Handle missing header
        if not contact_id:
            batch_update.append([""])
        else:
            updated = False  # Track if an update is made for this row
            for opportunity in opportunities:
                if (
                    "contact" in opportunity
                    and "id" in opportunity["contact"]
                    and opportunity["contact"]["id"] == contact_id
                ):
                    batch_update.append([opportunity.get("id", "")])
                    updated = True
                    break
            if not updated:
                batch_update.append([""])
    if "opportunity id" not in headers_mapping:
        opportunity_index = headers_mapping["processed"] + 1
        # use the header to figure out which column to update
        opportunity_id_range = (
            f"{chr(65 + opportunity_index-1)}2:{chr(65 + opportunity_index-1)}{len(batch_update) + 1}"
        )
        lds_sheet.insert_cols(values=[["Opportunity ID"]], col=opportunity_index)
    else:
        opportunity_id_range = f"{chr(65 + headers_mapping['opportunity id'])}2:{chr(65 + headers_mapping['opportunity id'])}{len(batch_update) + 1}"
    lds_sheet.batch_update(
        [
            {
                "range": opportunity_id_range,  # Update the range to a single column
                "values": batch_update,
            },
        ]
    )

    return True


def update_lds_opportunities(google_client=None):
    if not google_client:
        google_client = gspread.service_account_from_dict(GoogConfig.CREDENTIALS)
    mds_data = google_client.open_by_key(GoogConfig.MDS_SHEET_ID).get_worksheet(index=0).get_all_values()

    DB.create_retailers_table()
    insert_sheets_retailer_data(mds_data)

    # get locations from GoHighLevel using an agency token
    access_token = GoHighLevelConfig.AGENCY_ACCESS_TOKEN
    gohighlevel_locations = get_agency_locations_gohighlevel(access_token)

    # run through the gohighlevel locations, if there is an mds_link in the rgm_retailers table for the locationID, update the lead data sheet
    for location in gohighlevel_locations:
        location_key = location["apiKey"]
        location_id = location["id"]
        mds_link = DB.fetch_single_column("rgm_retailers", "lds_link", "locationId", location_id)
        if not mds_link:
            continue
        try:
            update_lds_with_opportunities(google_client, location_id, location_key, mds_link[0])
        except Exception as e:
            # prepare to create a clickup task
            create_clickup_task(location_id, mds_link[0])

    return True


def create_clickup_task(location_id, mds_link):
    title = "LDS-OPPORTUNITIES"
    description = f"Sub-account: {location_id} LDS: {mds_link}"
    # globals
    parent_id = "8678m9y2r"
    assignees = [57084868]
    OPERATIONS = List(id=ClickUpConfig.OPERATIONS_LIST_ID)
    OPERATIONS.create_task(
        values={"name": title, "description": description, "assignees": assignees, "parent": parent_id}
    )
    return True


def update_lds_with_opportunities(google_client, location_id, location_key, mds_link):
    # get pipelines for the location
    pipelines = get_location_pipelines_from_ghl(location_key)

    # get opportunities for each pipeline
    opportunities = [get_opportunities(location_key, pipeline["id"]) for pipeline in pipelines]

    # flatten the list of lists
    opportunities = [item for sublist in opportunities for item in sublist]
    try:
        lds_sheet, _ = open_lds(google_client, mds_link, location_id)

        # write the opportunity data to the lead data sheet
        write_opportunity_data_to_sheets(lds_sheet, opportunities)

        info_message = f"Updated Opps for Location: {location_id} LDS: {mds_link}"
        logging.info(info_message)
        print(info_message)
        DB.retailer_updated(location_id, 1)
    except Exception as e:
        print(f"Error updating Opps for Location: {location_id} LDS: {mds_link} Error: {e}")
        error_message = f"Error updating Opps for Location: {location_id} LDS: {mds_link} Error: {e}"
        logging.error(error_message)
        print(error_message)
        DB.retailer_updated(location_id, 2)
        return True


def get_agency_locations_gohighlevel(agency_access_token):
    url = "https://rest.gohighlevel.com/v1/locations/"

    payload = {}
    headers = {"Authorization": f"Bearer {agency_access_token}"}

    response = requests.request("GET", url, headers=headers, data=payload)
    verify_response(response.json())
    return response.json()["locations"]
