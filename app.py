from datetime import datetime
from flask import Flask, render_template, redirect, request, jsonify, send_file
import json
import pandas as pd
import pymysql.cursors
import sqlalchemy
from io import BytesIO
import string
from random import choice
import openpyxl
from re import escape
import base64


app = Flask(__name__)
app.secret_key = 'super secret key'
db_host = 'localhost'
db_name = 'service_api'
db_user = 'root'
db_pass = ''


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/advanced-search', methods=['GET'])
def advancedSearch():
    # Display all the query parameters including multiple values for the same key
    params = {}

    # Loop through the arguments and store all values for each key
    for key in request.args:
        values = request.args.getlist(key)  # Get all values for each key
        if len(values) == 1:
            values = values[0]  # If only one value, store it as a string
        else:
            values = list(values)  # If multiple values, store them as a list
        params[key] = values
    print(params)
    params_final = {
        "company_name_includes": params.get("companyNameIncludes", ""),
        "company_name_excludes": params.get("companyNameExcludes", ""),
        "company_status": params.get("status", []),
        "company_subtype": params.get("subtype", ""),
        "company_type": params.get("type", ""),
        "dissolved_from": f"{params.get('dissolvedToDay', '')}/{params.get('dissolvedToMonth', '')}/{params.get('dissolvedToYear', '')}",
        "dissolved_to": f"{params.get('dissolvedFromDay', '')}/{params.get('dissolvedFromMonth', '')}/{params.get('dissolvedFromYear', '')}",
        "incorporated_from": f"{params.get('incorporatedToDay', '')}/{params.get('incorporatedToMonth', '')}/{params.get('incorporatedToYear', '')}",
        "incorporated_to": f"{params.get('incorporatedFromDay', '')}/{params.get('incorporatedFromMonth', '')}/{params.get('incorporatedFromYear', '')}",
        "location": params.get("registeredOfficeAddress", ""),
        "sic_codes": params.get("sicCodes", [])
    }
    if params_final["dissolved_from"] == "//":
        params_final["dissolved_from"] = ""
    if params_final["dissolved_to"] == "//":
        params_final["dissolved_to"] = ""
    if params_final["incorporated_from"] == "//":
        params_final["incorporated_from"] = ""
    if params_final["incorporated_to"] == "//":
        params_final["incorporated_to"] = ""
    # final_data = getSearchResults(params_final)
    cleaned_params = params_final.copy()
    for key, value in params_final.items():
        if value == "" or value == []:
            del cleaned_params[key]
    if len(cleaned_params) == 0:
        return redirect('/')
    record_id = addToQueue(cleaned_params)
    return redirect(f'/records/{record_id}')


@app.route('/records/<record_id>')
def getRecordRedirect(record_id):
    return redirect(f'/records/{record_id}/1')


@app.route('/records/<record_id>/<page_number>')
def getRecord(record_id, page_number):
    if not page_number.isdigit():
        return redirect(f'/records/{record_id}/1')
    if int(page_number) <= 0:
        return redirect(f'/records/{record_id}/1')
    page_number = int(page_number)
    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_pass,
                                 database=db_name,
                                 cursorclass=pymysql.cursors.DictCursor)
    query = "SELECT * FROM queues WHERE record_id = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (record_id,))
        result = cursor.fetchone()
    connection.close()
    if result and result['status'] == 'pending':
        return render_template('pending.html')
    elif result and result['status'] == 'completed':
        result_timestamp = result['timestamp']
        days_ago = (datetime.now() - result_timestamp).days
        print(days_ago)
        offset = (page_number - 1) * 20
        connection = pymysql.connect(host=db_host,
                                     user=db_user,
                                     password=db_pass,
                                     database=db_name,
                                     cursorclass=pymysql.cursors.DictCursor)
        query = f"SELECT COUNT(*) FROM info WHERE record_id = %s"
        with connection.cursor() as cursor:
            cursor.execute(query, (record_id,))
            result = cursor.fetchone()
        if not result:
            results_count = 0
        else:
            results_count = result['COUNT(*)']
        query = f"SELECT * FROM info WHERE record_id = %s LIMIT 20 OFFSET {
            offset}"
        with connection.cursor() as cursor:
            cursor.execute(query, (record_id,))
            result = cursor.fetchall()
        connection.close()
        prev_page = f"/records/{record_id}/{page_number -
                                            1}" if page_number > 1 else ""
        next_page = f"/records/{record_id}/{page_number +
                                            1}" if page_number < results_count // 20 + 1 else ""
        max_pages = results_count // 20 + 1
        if page_number == max_pages:
            next_page = ""
        if page_number > max_pages:
            return redirect(f"/records/{record_id}/{max_pages}")
        for i, x in enumerate(result):
            officers_data = x.get('officers_data')
            if officers_data:
                officers_data = json.loads(officers_data)
                result[i]['officers_data'] = officers_data
        return render_template('record.html', records=result, record_id=record_id, results_count=results_count, prev_page=prev_page, next_page=next_page, current_page=page_number, max_pages=max_pages, days_ago=days_ago)
    else:
        return "Record doesn't exist or are you lost?", 404


@app.route('/download-excel/<record_id>')
def download_excel(record_id):
    columns = ['company_name', 'company_number', 'company_status', 'company_type', 'kind',
               'links', 'date_of_creation', 'registered_office_address', 'sic_codes', 'officer_name', 'officer_address']
    # Query data from MySQL using sqlalchemy
    connection = sqlalchemy.engine.create_engine(
        f'mysql+pymysql://{db_user}:{db_pass}@{db_host}/{db_name}').connect()
    query = "SELECT * FROM info WHERE record_id = '" + str(record_id) + "'"
    df = pd.read_sql(query, connection)
    df.drop(columns=['id'])
    connection.close()

    # Create a BytesIO object to save the Excel file in memory
    output = BytesIO()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(columns)
    for row in df.iterrows():
        company_name = row[1]['company_name']
        company_number = row[1]['company_number']
        company_status = row[1]['company_status']
        company_type = row[1]['company_type']
        kind = row[1]['kind']
        links = "https://find-and-update.company-information.service.gov.uk" + \
            row[1]['links']
        date_of_creation = row[1]['date_of_creation']
        registered_office_address = row[1]['registered_office_address']
        sic_codes = row[1]['sic_codes']
        officers_data = json.loads(row[1]['officers_data'])
        ws.append([company_name, company_number, company_status, company_type,
                  kind, links, date_of_creation, registered_office_address, sic_codes])
        for officer in officers_data:
            container = []
            for i in range(len(columns[:-2])):
                container.append("")
            container.append(officer.get('name'))
            container.append(officer.get('address'))
            ws.append(container)
    wb.save(output)
    output.seek(0)

    # Send the file to the user as a downloadable Excel file
    return send_file(output, download_name=f"{record_id}.xlsx", as_attachment=True, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@app.route('/update/<record_id>')
def setToPending(record_id):
    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_pass,
                                 database=db_name,
                                 cursorclass=pymysql.cursors.DictCursor)
    query = "DELETE FROM info WHERE record_id = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (record_id, ))
        connection.commit()
    query = "UPDATE queues SET status = 'pending', timestamp = CURRENT_TIMESTAMP WHERE record_id = %s"
    with connection.cursor() as cursor:
        cursor.execute(query, (record_id, ))
        connection.commit()
    connection.close()
    return redirect(f'/records/{record_id}')


def getRandomRecordID():
    string_data = string.ascii_letters + string.digits
    record_id = ''.join(choice(string_data) for _ in range(8))
    return record_id


def addToQueue(params):
    record_id = getRandomRecordID()
    connection = pymysql.connect(host=db_host,
                                 user=db_user,
                                 password=db_pass,
                                 database=db_name,
                                 cursorclass=pymysql.cursors.DictCursor)
    query = "SELECT * from queues WHERE `query` = %s"
    enc_query = base64.b64encode(json.dumps(params).encode()).decode()
    with connection.cursor() as cursor:
        cursor.execute(query, (enc_query, ))
        result = cursor.fetchone()
    if result:
        return result['record_id']
    query = "INSERT INTO queues (query, record_id, status) VALUES (%s, %s, %s)"
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (enc_query, record_id, 'pending'))
            connection.commit()
    return record_id


if __name__ == "__main__":
    app.run(debug=True, port=4000)
