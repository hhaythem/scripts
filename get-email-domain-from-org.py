import httplib, urllib, base64, sys, json, re, string, time
import MySQLdb
from MySQLdb import escape_string

def dbconnect():
    try:
        db = MySQLdb.connect(
            host = 'localhost',
            user = 'root',
            passwd = '',
            db = 'malaysiacrm'
        )
    except Exception as e :
        sys.exit('Cant connect to DB')

    return db

def execute_query_retrieve(data):
    handle_db = dbconnect()
    cursor = handle_db.cursor()
    cursor.execute(data)
    print data
    rows = cursor.fetchall()
    handle_db.close()
    return rows

def execute_query_update(data):
    handle_db = dbconnect()
    cursor = handle_db.cursor()
    cursor.execute(data)
    print ("Updated row : ")
    handle_db.commit()
    handle_db.close()

def update_contact_email_domain():
    index = 0
    sql_account_id = 'SELECT `contactid`, `accountid` FROM `vtiger_contactdetails` WHERE `accountid` !=  "" ;'
    rows = execute_query_retrieve(sql_account_id)
    for row in rows:
        contactid = row[0]
        accountid = row[1]
        sql_contact_tocheck='SELECT `contactid`,  FROM `vtiger_contactscf` WHERE `cf_829`="" AND `cf_847`="" AND `cf_1047` = "" AND `cf_815` != "" AND `cf_947` != "" AND `cf_947` != "http://" AND `cf_835` = "'+departement+'" ;'

def hunter_io_api(first_name, last_name, domain):
    
    api_key = ''#API KEY HERE
    params = urllib.urlencode({
        'api_key': api_key,
        'first_name': first_name,
        'last_name': last_name,
        'domain': domain,
        'company': ''
    })
    headers = {'content-type': 'application/x-www-form-urlencoded',
               'cache-control': 'no-cache'}
    conn = httplib.HTTPSConnection('api.hunter.io')
    conn.request("GET", "/v2/email-finder?%s" % params, "{body}", headers)
    response = conn.getresponse()
    data = response.read()
    data = json.loads(data.decode('utf-8'))
    print(data)
    conn.close()
    return data

#get contactid, domain cf_831 ,cf_751 nom from contacts_cf
#get firstname prenom contactdetails

def retrieve_and_update_email():
    index = 0
    departement_list =['Engineering', 'QA/QC', 'Purchase', 'Operations', 'MD/President/CFO/Chairman/Founder', 'Board Of Directors', 'Production/Maintenance/Plant', 'Project & Planning', 'Senior Management - General', 'Middle Management - General', 'Health & Safety']
    for departement in departement_list:
        print departement
        # get contactid, domain cf_947 ,cf_751 nom from contacts_cf
        sql_statement = 'SELECT `contactid`, `cf_751`, `cf_947` FROM `vtiger_contactscf` WHERE `cf_829`="" AND `cf_847`="" AND `cf_1047` = "" AND `cf_815` != "" AND `cf_947` != "" AND `cf_947` != "http://" AND `cf_835` = "'+departement+'" ;'
        rows = execute_query_retrieve(sql_statement)
        for row in rows:
            index += 1
            #if index <= 4777:
            contactid = str(row[0])
            last_name = str(row[1])
            last_name = re.sub(r'[^a-zA-Z]', '', last_name)
            domain = str(row[2])
            sql_get_firstname = 'SELECT `firstname` FROM `vtiger_contactdetails` WHERE `contactid` = "'+contactid+'";'
            fname = execute_query_retrieve(sql_get_firstname)
            for frow in fname:

                first_name = str(frow[0])
                first_name = re.sub(r'[^a-zA-Z]', '', first_name)
                data = hunter_io_api(first_name, last_name, domain)
                #hunter_email = cf_1047, hunter_score = cf_1049, status = cf_1051
                try:
                    email = data['data']['email']
                    score = data['data']['score']
                    Status = 'Found'
                except:
                    email = '-'
                    score = "0%"
                    Status = 'Not Found'
                update_contactscf_data = "UPDATE `vtiger_contactscf` SET `cf_1047`='" + str(email) + "', `cf_1049`='" + str(score) + "', `cf_1051`='" + str(Status) + "'  WHERE `contactid`='" + str(contactid) + "';"
                execute_query_update(update_contactscf_data)
                print contactid
                print ("Inserted data Index: ", index)
            #else:
                #break



if __name__ == '__main__':
    #hunter_io_api('Changcheng','Liu','http://www.petronas.com/')
    retrieve_and_update_email()