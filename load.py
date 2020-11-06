#!/usr/local/bin/python3

import os
import sys
import requests
import mysql.connector
import pycountry
import pyparsing
import time
import datetime

mydb = mysql.connector.connect(host="localhost",user="wna",password="",database="covid")
mycursor = mydb.cursor()
amonth = 30 #debug 30 is right
url = {}
url['mobility'] = "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
url['cases'] = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"

static_country_code = {
    "US":"US",
    "Guinea":"GN",
    "Spain":"ES",
    "Congo (Brazzaville)":"CG",
    "Cape Verde":"CV",
    "Korea, South":"KR",
    "Aruba":"AW",
    "Hong Kong":"HK",
    "Puerto Rico":"PR",
    "Mexico":"MX",
    "Oman":"OM",
    "Niger":"NE",
    "West Bank and Gaza":"PS",
    "China":"CN",
    "Ireland":"IE",
    "United States":"US",
    "Mali":"ML",
    "Luxembourg":"LU",
    "Myanmar (Burma)":"MM",
    "South Korea":"KR",
    "MS Zaandam":"GL",
    "India":"IN",
    "Laos":"LA",
    "Chad":"TD",
    "Jordan":"JO",
    "Peru":"PE",
    "Diamond Princess":"GL",
    "Taiwan":"TW",
    "Congo (Kinshasa)":"CD",
    "Georgia":"GE",
    "Taiwan*":"TW",
    "Iran":"IR",
    "Cuba":"CU",
    "Burma":"MM",
    "Sudan":"SD",
    "Dominica":"DM",
}

extra_country_code = {
    "occupied Palestinian territory":"PS",
    "mainland china":"CN",
    "Macau":"MO",
    "Mainland China":"CN",
    "UK":"GB",
    "Others":"GL",
    "North Ireland":"GB",
    "Martinique":"MQ",
    "Republic of Ireland":"IE",
    "St. Martin":"MF",
    "Iran (Islamic Republic of)":"IR",
    "Taipei and environs":"TW",
    "Cruise Ship":"GL",
    "Reunion":"GL",
    "Channel Islands":"GL",
    "Guadeloupe":"GP",
    "Jersey":"JE",
    "Curacao":"CW",
    "Guam":"GU",
    "Mayotte":"YT",
    "Gambia, The":"GM",
    "Bahamas, The":"BS",
    "East Timor":"TL",
}        

insert_cmd = {
        "country_code":"insert into country_code (name,code) values (%s,%s)",
        "region_id":"insert into region_id (country_code,sub_region1,sub_region2,region_id) values (%s,%s,%s,%s)",
        "mobility":"insert into mobility (region_id,timestamp,retail,grocery,parks,transit,workplace,resident) values (%s,%s,%s,%s,%s,%s,%s,%s)",
        "cases":"insert into cases (region_id,timestamp,confirmed,deaths) values (%s,%s,%s,%s)",
}

def get_date_arr(table_name):
    A = []
    mycursor.execute("select max(timestamp) from %s" % table_name)
    init_date = int(time.mktime(time.strptime("2020-11-01 00:00:00",'''%Y-%m-%d %H:%M:%S''')))
    target_date = int(time.time())
#    target_date = int(time.mktime(time.strptime("2020-11-01 00:00:00",'''%Y-%m-%d %H:%M:%S''')))
    max_date = init_date
    for each_date in map(lambda x: int(to_epoch(x[0])),filter(lambda x:x[0] != None ,mycursor.fetchall())):
        if max_date < each_date: max_date = each_date
    each_date = max_date + 86400
    while(each_date < target_date):
        A.append(time.strftime("%Y-%m-%d",time.gmtime(each_date)))
        each_date += 86400
    return A

def to_epoch(date):
    return (date - datetime.date(1970,1,1)).total_seconds()

def fetch_parse(dest,Dates):
    return_text = []
    if dest == 'mobility':
        resp = requests.get(url[dest])
#        resp = requests.get("https://www.naver.com")
        if resp.ok:
            if len(Dates) >= amonth:
                return resp.text.split("\n")
            else:
                for each_line in resp.text.split("\n"): 
#                for each_line in os.popen("cat hihi.csv").read().split("\n"): #debug
                    if len(list(filter(lambda x: x in each_line,['date'] + Dates))) > 0:
                        return_text.append(each_line)
    if dest == 'cases':
        for each_d in map(lambda x: time.strftime('''%m-%d-%Y''',time.strptime(x,'''%Y-%m-%d''')),Dates):
#            if each_d != '10-21-2020': continue #debug
            resp = requests.get(url[dest] + each_d + ".csv")
            if resp.ok:
                return_text += resp.text.split("\n")
    return return_text

def fill_existing(country_code,uniq_id,region_id,mobility_set,cases_set):
    fill_country_code(country_code)
    fill_region_id(region_id,uniq_id)
    fill_mobility(mobility_set)
    fill_cases(cases_set)
    return

def parse(records,tables,country_code,uniq_id,region_id,mobility_set,cases_set):
    pos = {}
    for each_table in ('country_code','region_id','mobility','cases'):
        if each_table not in tables:
            tables[each_table] = {}
    try:
        max_rid = max(uniq_id)
    except:
        max_rid = 0
    for each_line in records:
        if len(pos) == 0:
            for i,fn in enumerate(each_line.split(",")):
                if fn[0:6] == 'Admin2': fn = 'sub_region_2'
                if fn[0:14] in ('Province/State','Province_State'): fn = 'sub_region_1'
                if fn[0:14] in ('Country/Region','Country_Region'): fn = 'country_region'
                if fn[0:11] in ('Last_Update','Last Update'): fn = 'date'
                pos[fn.strip()] = i
        else:
            values = pyparsing.commaSeparatedList.parseString(each_line).asList()
            if len(values) < len(pos.keys()): continue
#            if values[pos['date']] == 'Last_Update': continue
            for i,v in enumerate(values):
                if len(v) >= 2 and v[0] == '"' and v[-1] == '"': values[i] = v[1:len(v)-1]
            if values[pos['country_region']] not in country_code:
                if 'country_region_code' in pos:
                    country_code[values[pos['country_region']]] = values[pos['country_region_code']]
                else:
                    try:
                        a = pycountry.countries.search_fuzzy(values[pos['country_region']])
                        if len(a) == 1:
                            country_code[values[pos['country_region']]] = a[0].alpha_2
                        else:
                            print (",".join(lambda x: x.alpha_2,a))
                            sys.exit(0)
                    except:
                        try:
                            country_code[values[pos['country_region']]] = extra_country_code[values[pos['country_region']]]
                        except:
                            print ("HOHO",values[pos['country_region']])
                            continue
                tables['country_code'][values[pos['country_region']]] = country_code[values[pos['country_region']]]

            cc = country_code[values[pos['country_region']]]
            sub1 = sub2 = ""
            if 'sub_region_1' in pos:
                sub1 = values[pos['sub_region_1']]
            if 'sub_region_2' in pos:
                sub2 = values[pos['sub_region_2']]
            if ' County' ==  sub2[len(sub2)-7:len(sub2)]: sub2 = sub2[0:len(sub2)-7]
            if (cc,sub1,sub2) not in region_id:
                max_rid += 1
                region_id[(cc,sub1,sub2)] = max_rid
                tables['region_id'][(cc,sub1,sub2)] = max_rid
                uniq_id.add(max_rid)
            rid = region_id[(cc,sub1,sub2)]
            timestamp = get_timestamp(values[pos['date']])
                    
            if 'retail_and_recreation_percent_change_from_baseline' in pos and (rid,timestamp) not in mobility_set:
                tables['mobility'][(rid,timestamp)] = (
                        values[pos['retail_and_recreation_percent_change_from_baseline']],
                        values[pos['grocery_and_pharmacy_percent_change_from_baseline']],
                        values[pos['parks_percent_change_from_baseline']],
                        values[pos['transit_stations_percent_change_from_baseline']],
                        values[pos['workplaces_percent_change_from_baseline']],
                        values[pos['residential_percent_change_from_baseline']]
                )
            if 'Confirmed' in pos and (rid,timestamp) not in cases_set:
                tables['cases'][(rid,timestamp)] = (
                        values[pos['Confirmed']],
                        values[pos['Deaths']]
                )
    return

def get_timestamp(timestr):
    try:
        return time.strftime('''%Y-%m-%d''',time.strptime(timestr,'''%Y-%m-%d'''))
    except:
        pass
    try:
        return time.strftime('''%Y-%m-%d''',time.strptime(timestr,'''%m/%d/%Y %H:%M'''))
    except:
        pass
    try:
        return time.strftime('''%Y-%m-%d''',time.strptime(timestr,'''%m/%d/%y %H:%M'''))
    except:
        pass
    try:
        return time.strftime('''%Y-%m-%d''',time.strptime(timestr,'''%Y-%m-%d %H:%M:%S'''))
    except:
        pass
    try:
        return time.strftime('''%Y-%m-%d''',time.strptime(timestr,'''%Y-%m-%dT%H:%M:%S'''))
    except Exception as err:
        print (err)
        sys.exit(0)

def fill_country_code(country_code):
    mycursor.execute("select * from country_code")
    for name, code in mycursor.fetchall():
        if name not in country_code:
            country_code[name] = code

def fill_region_id(region_id,uniq_id):
    mycursor.execute("select * from region_id")
    for code,sub_region1,sub_region2,rid in mycursor.fetchall():
        region_id[(code,sub_region1,sub_region2)] = rid
        uniq_id.add(rid)

def fill_mobility(mobility_set):
    mycursor.execute("select distinct region_id,timestamp from mobility")
    for rid, timestamp in mycursor.fetchall():
        mobility_set.add((rid,timestamp))

def fill_cases(mobility_set):
    mycursor.execute("select region_id,timestamp from cases")
    for rid, timestamp in mycursor.fetchall():
        mobility_set.add((rid,timestamp))

def insert_table(tables):
    for dest in tables.keys():
        total_val = []
        sql = insert_cmd[dest];
        for each_record in tables[dest].keys():
            val = []
            if dest == 'country_code':
                val.append(each_record)
            else:
                for each_key in each_record:
                    val.append(each_key)
            if dest in ('country_code','region_id'):
                each_value = tables[dest][each_record]
                val.append(each_value)
            else:
                for each_value in tables[dest][each_record]:
                    if each_value == '' and dest in('mobility','cases') :
                        each_value = 0
                    val.append(is_int(each_value))

            total_val.append(tuple(val))

        if len(total_val) > 0:
            try:
                mycursor.executemany(sql,total_val)
                mydb.commit()
                print (mycursor.rowcount,"was inserted")
            except Exception as err:
                print (err)
                print (sql, total_val)

def is_int(s):
    try:
        return int(s)
    except:
        return s

def main():
    country_code = {}
    uniq_id = set([])
    region_id = {}
    mobility_set = set([])
    cases_set = set([])
    tables = {}
    fill_existing(country_code,uniq_id,region_id,mobility_set,cases_set)
    if 'South Korea' not in country_code:
        a = {}
        a['country_code'] = static_country_code
        insert_table(a)
        fill_country_code(country_code)
    parse(fetch_parse('mobility',get_date_arr('mobility')),tables,country_code,uniq_id,region_id,mobility_set,cases_set)
#    print ("\n".join(map(lambda x: "%s:%d" % (",".join(x),tables['region_id'][x]),tables['region_id'].keys())))
    Dates = get_date_arr('cases')
    for each_d in Dates:
        parse(fetch_parse('cases',[each_d]),tables,country_code,uniq_id,region_id,mobility_set,cases_set)
#    print ("\n".join(map(lambda x: "%s:%s" % ("%d,%s" % (x[0],x[1]),",".join(tables['cases'][x])),tables['cases'].keys())))
    insert_table(tables)

if __name__ == '__main__':
    main()
