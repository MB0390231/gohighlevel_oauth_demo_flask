import requests
from oauth_flask.config import CLIENT_ID, CLIENT_SECRET
from oauth_flask.sqlite_db import SQLiteDB

DB = SQLiteDB()


def verify_response(response):
    if "error" in response:
        print(response)
        return False
    return True


def refresh_tokens():
    """refreshes all of the tokens in the api_data table"""
    data = DB.fetch_all_records("api_data")

    for row in data:
        refresh_token = row[6]
        refresh_one_token(refresh_token)

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
        if lds_link == "" or location_id == "":
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
            print(f"Error: {response.status_code}")
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

        lead_data_sheet = google_client.open_by_url(lds_link).get_worksheet(index=0)
        worksheet_values = lead_data_sheet.get_all_values()

        # map the headers
        headers_mapping = {header.lower().rstrip(): index for index, header in enumerate(worksheet_values[0])}

        # ensure the proper headers are present
        missing_headers = verify_headers(
            ["phone", "email", "first name", "last name", "contact id", "location id"], worksheet_values
        )
        if missing_headers:
            with open("missing_headers.txt", "a") as f:
                # print and write out the list of missing headers from the missing_headers list of strings
                f.write(f"Missing headers in location {location_id}, sheet {lds_link}, headers: {missing_headers}\n")
                print(f"Missing headers in location {location_id}, sheet {lds_link}, headers: {missing_headers}")
            continue

        contact_id_batch, location_id_batch = create_batch(location_id, worksheet_values, headers_mapping)

        update_location_contact_ids(location_id_batch, contact_id_batch, lead_data_sheet, location_id)
        print(f"Updated location {location_id}")
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

        contact_record = DB.attempt_contact_retrieval(location_id, phone_number, email, first_name, last_name)

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

    empty_rows = []
    for idx, contact_id in enumerate(contact_id_batch, start=1):
        if contact_id == "":
            empty_rows.append(idx)

    consecutive_empty_rows = 0
    with open("missing_contacts", "a") as f:
        f.write(f"Location {location_id}\n")
        for idx, contact_id in enumerate(contact_id_batch, start=1):
            if contact_id == "":
                consecutive_empty_rows += 1
                f.write(f"  Row {idx + 1}\n")
                if consecutive_empty_rows >= 3:
                    break
            else:
                consecutive_empty_rows = 0

    return True
