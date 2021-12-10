#Emilia O'Brien: Final Project
# Project Partner: Dani Vykydal
# Group name: Danmilia
import os
import unittest
import sqlite3
import json
import requests
import datetime

def readDataFromFile(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    return file_data
 

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn
     
def createDates(conn, cur):
    cur.execute("Create TABLE IF NOT EXISTS Dates (ID INTEGER PRIMARY KEY, Date DATE)")
    starting_day= datetime.date(2020, 6,1)
    daysNum=100
    days_list=[]
    for day in range(daysNum):
        date=(starting_day+ datetime.timedelta(days=day)).isoformat()
        days_list.append(date)
    for i in range(len(days_list)):
        cur.execute("INSERT OR IGNORE INTO Dates (ID, Date) VALUES (?,?)", (i+1, days_list[i]))
    conn.commit()
    return(days_list)

    
    
def getData(conn, cur):
    count=0
    date_count=0
    # ^^^ Do you need this???
    baseurl='https://api.covid19tracker.ca/reports?date={}'
    cur.execute("CREATE TABLE IF NOT EXISTS Canada(Date_id INTEGER PRIMARY KEY,Cases INTEGER, Deaths INTEGER, Tests INTEGER, Vaccinations INTEGER, Total_Cases INTEGER, Total_Deaths INTEGER)")
    conn.commit()
    cur.execute("SELECT COUNT(Date_id) FROM Canada")
    table_length= cur.fetchone()[0]  
    dates= createDates(conn, cur)
    if table_length > 75:
        for i in range(100-table_length):
            if 100- table_length <0:
                break
            else:
                cur.execute("SELECT MAX(Date_id) FROM Canada")
                max_date=cur.fetchone()[0]
                # max_date_index= dates.index(max_date)
                requestsurl= baseurl.format(dates[max_date])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date=?", (day_data['data'][0]['date'],))
                id_date= cur.fetchone()[0]
                insert_tup= (id_date, day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (Date_id, Cases, Deaths, Tests, Vaccinations, Total_Cases, Total_Deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()
    else:
        for i in range(25): 
            cur.execute("SELECT COUNT(Date_id) FROM Canada")
            table_length= cur.fetchone()[0]   
            if table_length <=25:
                cur.execute("SELECT MAX(Date_id) FROM Canada")
                max_date=cur.fetchone()[0]
                if max_date==None:
                    requestsurl= baseurl.format(dates[count])
                    count+=1
                else:
                    requestsurl= baseurl.format(dates[max_date])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date=?", (day_data['data'][0]['date'],))
                id_date= cur.fetchone()[0]
                insert_tup= (id_date, day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (Date_id, Cases, Deaths, Tests, Vaccinations, Total_Cases, Total_Deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()
            else:
                cur.execute("SELECT MAX(Date_id) FROM Canada")
                max_date=cur.fetchone()[0]
                requestsurl= baseurl.format(dates[max_date])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date=?", (day_data['data'][0]['date'],))
                id_date= cur.fetchone()[0]
                insert_tup= (id_date, day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (Date_id, Cases, Deaths, Tests, Vaccinations, Total_Cases, Total_Deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()


def get_US_Data(conn, cur):
    day_count = 0
    baseurl =  'https://api.covidtracking.com/v2/us/daily/{}.json'
    cur.execute("CREATE TABLE IF NOT EXISTS US (date_id INTEGER PRIMARY KEY, total_cases INTEGER, total_deaths INTEGER, total_tests INTEGER, current_hospitalized INTEGER, current_in_icu INTEGER, current_on_ventilator INTEGER)")
    conn.commit()
    cur.execute("SELECT COUNT (date_id) FROM US")
    table_length= cur.fetchone()[0]  
    dates = createDates(conn, cur)
    # ^^^ Make this be in another function?
    for i in range(4):
        for i in range(25):
            cur.execute("SELECT COUNT (date_id) FROM US")
            table_length = cur.fetchone()[0]   
            cur.execute("SELECT MAX (date_id) FROM US")
            if table_length <= 25:
                cur.execute("SELECT MAX (date_id) FROM US")
                max_date = cur.fetchone()[0]
                if (max_date == None):
                    date = dates[day_count]
                    requestsurl = baseurl.format(date)
                    day_count += 1
                else:
                    max_date = dates[max_date]
                    requestsurl= baseurl.format(max_date)
                day = requests.get(requestsurl).content
                day_data = json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date = ?", (day_data['data']['date'],))
                id_date = cur.fetchone()[0]
                day_tup = (id_date, day_data['data']['cases']['total']['value'], day_data['data']['outcomes']['death']['total']['value'], day_data['data']['testing']['total']['value'], day_data['data']['outcomes']['hospitalized']['currently']['value'], day_data['data']['outcomes']['hospitalized']['in_icu']['currently']['value'], day_data['data']['outcomes']['hospitalized']['on_ventilator']['currently']['value'])
                cur.execute("INSERT OR IGNORE INTO US (date_id, total_cases, total_deaths, total_tests, current_hospitalized, current_in_icu, current_on_ventilator) VALUES (?,?,?,?,?,?,?)", day_tup)
                conn.commit()
            elif table_length < 100:
                cur.execute("SELECT MAX (date_id) FROM US")
                max_date = cur.fetchone()[0]
                requestsurl = baseurl.format(dates[max_date])
                day = requests.get(requestsurl).content
                day_data = json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date = ?", (day_data['data']['date'],))
                id_date = cur.fetchone()[0]
                day_tup = (id_date, day_data['data']['cases']['total']['value'], day_data['data']['outcomes']['death']['total']['value'], day_data['data']['testing']['total']['value'], day_data['data']['outcomes']['hospitalized']['currently']['value'], day_data['data']['outcomes']['hospitalized']['in_icu']['currently']['value'], day_data['data']['outcomes']['hospitalized']['on_ventilator']['currently']['value'])
                cur.execute("INSERT OR IGNORE INTO US (date_id, total_cases, total_deaths, total_tests, current_hospitalized, current_in_icu, current_on_ventilator) VALUES (?,?,?,?,?,?,?)", day_tup)
                conn.commit()

def main():
    cur, conn = setUpDatabase('Covid.db')
    getData(conn, cur)
    get_US_Data(conn, cur)
    createDates(conn, cur)
        #  How to set up conn and cur?
        # could we do this before we make tables? to use between??

if __name__ == "__main__":
    main()


        

# Need to make a date, and a date range table