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
    # Copied these from hmwk 7- check if I wrote or they were provided

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn
     # Copied these from hmwk 7- check if I wrote or they were provided
def createDates():
    starting_day= datetime.date(2020, 6,1)
    daysNum=100
    days_list=[]
    for day in range(daysNum):
        date=(starting_day+ datetime.timedelta(days=day)).isoformat()
        days_list.append(date)
    return(days_list)
    
    # Make the list of 100 dates that I want to use to create request urls for
    # Can also use range of days
    # find max date in database- use MAX sql
def setUpNewsTable(data, cur, conn):
        cur.execute("CREATE TABLE IF NOT EXISTS Canada(Date DATE,Cases INTEGER, Deaths INTEGER, Tests INTEGER, Vaccinations INTEGER, Total_Cases INTEGER, Total_Deaths INTEGER)")
        conn.commit()
        dates= createDates()
    
        # Need to add the items into the table
def getData(conn, cur):
    count=0
    date_count=0
    baseurl='https://api.covid19tracker.ca/reports?date={}'
    cur.execute("CREATE TABLE IF NOT EXISTS Canada(Date DATE PRIMARY KEY,Cases INTEGER, Deaths INTEGER, Tests INTEGER, Vaccinations INTEGER, Total_Cases INTEGER, Total_Deaths INTEGER)")
    conn.commit()
    cur.execute("SELECT COUNT(Date) FROM Canada")
    table_length= cur.fetchone()[0]  
    dates= createDates()
    if table_length > 75:
        for i in range(100-table_length):
            if 100- table_length <0:
                break
            else:
                cur.execute("SELECT MAX(Date) FROM Canada")
                max_date=cur.fetchone()[0]
                max_date_index= dates.index(max_date)+1
                requestsurl= baseurl.format(dates[max_date_index])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                insert_tup= (day_data['data'][0]['date'], day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (Date, Cases, Deaths, Tests, Vaccinations, Total_Cases, Total_Deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()
    else:
        for i in range(25): 
            cur.execute("SELECT COUNT(Date) FROM Canada")
            table_length= cur.fetchone()[0]   
            if table_length <=25:
                cur.execute("SELECT MAX(Date) FROM Canada")
                max_date=cur.fetchone()[0]
                if max_date==None:
                    requestsurl= baseurl.format(dates[count])
                    count+=1
                else:
                    max_date_index= dates.index(max_date)+1
                    requestsurl= baseurl.format(dates[max_date_index])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                insert_tup= (day_data['data'][0]['date'], day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (Date, Cases, Deaths, Tests, Vaccinations, Total_Cases, Total_Deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()
            else:
                cur.execute("SELECT MAX(Date) FROM Canada")
                max_date=cur.fetchone()[0]
                max_date_index= dates.index(max_date)+1
                requestsurl= baseurl.format(dates[max_date_index])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                insert_tup= (day_data['data'][0]['date'], day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (Date, Cases, Deaths, Tests, Vaccinations, Total_Cases, Total_Deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()



def main():
    cur, conn = setUpDatabase('Covid.db')
    getData(conn, cur)
        #  How to set up conn and cur?

if __name__ == "__main__":
    main()


        




# Shared table for Country/ state area