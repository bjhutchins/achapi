import json
from time import sleep
import traceback
import pymysql.cursors
import base64
import requests

db_host = 'localhost'
db_name = 'service_api'
db_user = 'root'
db_pass = ''
token = "9504e4fc-4c9d-4953-95a3-444ded8f2483"
enc_token = base64.b64encode(f"{token}:".encode()).decode('utf-8')


def getCompanyOfficers(company_number):
    link = f"https://api.company-information.service.gov.uk/company/{
        company_number}/officers"
    params = {
        'items_per_page': '5000'
    }
    headers = {
        "Authorization": f"Basic {enc_token}"
    }
    try:
        resp = requests.get(link, headers=headers, params=params).json()
    except:
        print("Failed to open {}".format(link))
        return []
    officers = resp.get('items', [])
    active_officers = []
    for officer in officers:
        # skipping inactive officers
        if officer.get('resigned_on'):
            continue
        officer_name = officer.get('name')
        address_fields = []
        for field in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code']:
            value = officer.get('address').get(field)
            if value:
                address_fields.append(value)
        address = ', '.join(address_fields)
        active_officers.append({
            'name': officer_name,
            'address': address
        })
    return active_officers


def getSearchResults(params):
    link = "https://api.company-information.service.gov.uk/advanced-search/companies"

    # params = {
    #     'company_status': 'active',
    #     'location': 'Poole',
    #     'sic_codes': ['98000'],
    #     'size': '5000'
    # }
    params['size'] = '5000'
    start_index = 0
    items_container = []
    total_hits = None
    while True:
        print("Start index: {}".format(start_index))
        params['start_index'] = str(start_index)
        headers = {
            "Authorization": f"Basic {enc_token}"
        }
        try:
            resp = requests.get(link, headers=headers,
                                params=params).json()
        except:
            print("Failed to open {}".format(link))
            return
        if total_hits is None:
            total_hits = resp.get('hits')
            print("Total records: {}".format(total_hits))
            if not total_hits:
                break
        companies = resp.get('items', [])
        for i, company in enumerate(companies):
            company_number = company.get('company_number')
            print("[{}/{}] Company Number: {}".format(i+1, total_hits, company_number))
            print("Getting active officers info ...")
            officers = getCompanyOfficers(company_number)
            company['officers'] = officers
            items_container.append(company)
        if total_hits <= start_index+5000:
            break
        start_index += 5000
    return items_container


def monitorQueue():
    try:
        connection = pymysql.connect(host=db_host,
                                     user=db_user,
                                     password=db_pass,
                                     database=db_name,
                                     cursorclass=pymysql.cursors.DictCursor)
    except:
        print("Cannot connect to the database")
        return
    query = "SELECT * FROM queues WHERE status = 'pending'"
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
    except:
        print("Could not fetch data")
        return
    try:
        connection.close()
    except:
        print("Could not close connection")
    for result in results:
        record_id = result['record_id']
        params = result['query']
        params = base64.b64decode(params).decode('utf-8')
        try:
            params_final = eval(params)
        except:
            continue
        print("Processing record {}".format(record_id))
        final_data = getSearchResults(params_final)
        connection = pymysql.connect(host=db_host,
                                     user=db_user,
                                     password=db_pass,
                                     database=db_name,
                                     cursorclass=pymysql.cursors.DictCursor)
        query = "INSERT INTO info (record_id, company_name, company_number, company_status, company_type, kind, links, date_of_creation, registered_office_address, sic_codes, officers_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        with connection.cursor() as cursor:
            for company in final_data:
                print("Getting company active officers for company {}".format(company.get('company_name')))
                active_officers = company['officers']
                address_fields = []
                for field in ['address_line_1', 'address_line_2', 'locality', 'region', 'postal_code']:
                    value = company.get('registered_office_address').get(field)
                    if value:
                        address_fields.append(value)
                full_address = ', '.join(address_fields)
                cursor.execute(query, (record_id, company.get('company_name'),
                                       company.get('company_number'),
                                       company.get('company_status'),
                                       company.get('company_type'),
                                       company.get('kind'),
                                       company.get('links', {}).get('company_profile'),
                                       company.get('date_of_creation'),
                                       full_address,
                                       company.get('sic_codes', [""])[0],
                                       json.dumps(active_officers)
                                       ))
        query = "UPDATE queues SET status = 'completed' WHERE record_id = %s"
        with connection.cursor() as cursor:
            cursor.execute(query, (record_id, ))
        connection.commit()
        connection.close()


if __name__ == "__main__":
    print("Monitoring queue has started")
    while True:
        try:
            monitorQueue()
        except:
            traceback.print_exc()
        sleep(1)
