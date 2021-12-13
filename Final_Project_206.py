#Emilia O'Brien: Final Project
# Project Partner: Dani Vykydal
# Group name: Danmilia
import os
import unittest
import sqlite3
import json
import requests
import datetime
import csv
import matplotlib.pyplot as plt
import numpy as np
 
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
    
def getDataCanada(conn, cur):
    count=0
    baseurl='https://api.covid19tracker.ca/reports?date={}'
    cur.execute("CREATE TABLE IF NOT EXISTS Canada(date_id INTEGER PRIMARY KEY, daily_cases INTEGER, daily_deaths INTEGER, daily_tests INTEGER, vaccinations INTEGER, CA_total_cases INTEGER, CA_total_deaths INTEGER)")
    conn.commit()
    cur.execute("SELECT COUNT(date_id) FROM Canada")
    table_length= cur.fetchone()[0]  
    dates= createDates(conn, cur)
    if table_length > 75:
        for i in range(100-table_length):
            if 100- table_length <0:
                break
            else:
                cur.execute("SELECT MAX(date_id) FROM Canada")
                max_date=cur.fetchone()[0]
                requestsurl= baseurl.format(dates[max_date])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date=?", (day_data['data'][0]['date'],))
                id_date= cur.fetchone()[0]
                insert_tup= (id_date, day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (date_id, daily_cases, daily_deaths, daily_tests, vaccinations, CA_total_cases, CA_total_deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()
    else:
        for i in range(25): 
            cur.execute("SELECT COUNT(date_id) FROM Canada")
            table_length= cur.fetchone()[0]   
            if table_length <=25:
                cur.execute("SELECT MAX(date_id) FROM Canada")
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
                cur.execute("INSERT OR IGNORE INTO Canada (date_id, daily_cases, daily_deaths, daily_tests, vaccinations, CA_total_cases, CA_total_deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()
            else:
                cur.execute("SELECT MAX(date_id) FROM Canada")
                max_date=cur.fetchone()[0]
                requestsurl= baseurl.format(dates[max_date])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date=?", (day_data['data'][0]['date'],))
                id_date= cur.fetchone()[0]
                insert_tup= (id_date, day_data['data'][0]['change_cases'], day_data['data'][0]['change_fatalities'], day_data['data'][0]['change_tests'], day_data['data'][0]['change_vaccinations'], day_data['data'][0]['total_cases'], day_data['data'][0]['total_fatalities'])
                cur.execute("INSERT OR IGNORE INTO Canada (date_id, daily_cases, daily_deaths, daily_tests, vaccinations, CA_total_cases, CA_total_deaths) VALUES (?,?,?,?,?,?,?)", insert_tup)
                conn.commit()


def get_US_Data(conn, cur):
    day_count = 0
    baseurl =  'https://api.covidtracking.com/v2/us/daily/{}.json'
    cur.execute("CREATE TABLE IF NOT EXISTS US (date_id INTEGER PRIMARY KEY, US_total_cases INTEGER, US_total_deaths INTEGER, total_tests INTEGER, current_hospitalized INTEGER, current_in_icu INTEGER, current_on_ventilator INTEGER)")
    conn.commit()
    cur.execute("SELECT COUNT (date_id) FROM US")
    table_length= cur.fetchone()[0]  
    dates = createDates(conn, cur)
    if table_length > 75:
        for i in range(100-table_length):
            if 100- table_length <0:
                break
            else:
                cur.execute("SELECT MAX(date_id) FROM US")
                max_date=cur.fetchone()[0]
                requestsurl= baseurl.format(dates[max_date])
                day= requests.get(requestsurl).content
                day_data=json.loads(day)
                cur.execute("SELECT ID from Dates WHERE Date=?", (day_data['data'][0]['date'],))
                id_date= cur.fetchone()[0]
                day_tup = (id_date, day_data['data']['cases']['total']['value'], day_data['data']['outcomes']['death']['total']['value'], day_data['data']['testing']['total']['value'], day_data['data']['outcomes']['hospitalized']['currently']['value'], day_data['data']['outcomes']['hospitalized']['in_icu']['currently']['value'], day_data['data']['outcomes']['hospitalized']['on_ventilator']['currently']['value'])
                cur.execute("INSERT OR IGNORE INTO US (date_id, US_total_cases, US_total_deaths, total_tests, current_hospitalized, current_in_icu, current_on_ventilator) VALUES (?,?,?,?,?,?,?)", day_tup)
                conn.commit()
    else:
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
                cur.execute("INSERT OR IGNORE INTO US (date_id, US_total_cases, US_total_deaths, total_tests, current_hospitalized, current_in_icu, current_on_ventilator) VALUES (?,?,?,?,?,?,?)", day_tup)
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
                cur.execute("INSERT OR IGNORE INTO US (date_id, US_total_cases, US_total_deaths, total_tests, current_hospitalized, current_in_icu, current_on_ventilator) VALUES (?,?,?,?,?,?,?)", day_tup)
                conn.commit()

def average_cases(cur, filename):
    average_cases_list_CA = []
    average_cases_list_US = []
    count = 0
    cur.execute('SELECT Canada.CA_total_cases, US.US_total_cases FROM Canada JOIN US ON Canada.date_id = US.date_id')
    cases = cur.fetchall()
    for i in range(10):
        cases_list_CA = []
        cases_list_US = []
        for cases_tup in cases[count:count+10]:
            cases_list_CA.append(cases_tup[0])
            cases_list_US.append(cases_tup[1])
        cases_sum_CA, cases_sum_US = sum(cases_list_CA), sum(cases_list_US)
        average_cases_CA, average_cases_US = (cases_sum_CA / 10), (cases_sum_US / 10)
        average_cases_list_CA.append(average_cases_CA)
        average_cases_list_US.append(average_cases_US)
        count += 10

    with open(filename, "w", newline="") as outFile:
        csv_writer = csv.writer(outFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL) 
        header = ['Average Total Canada COVID19 Cases over Ten Period Intervals', 'Average Total U.S. COVID19 Cases over Ten Period Intervals']
        csv_writer.writerow(header)
        zip_object = zip(average_cases_list_CA, average_cases_list_US)
        for average_CA, average_US in zip_object:
            file_line = []
            file_line.append(str(average_CA))
            file_line.append(str(average_US))
            csv_writer.writerow(file_line) 
    
    return average_cases_list_CA, average_cases_list_US


def average_deaths(cur, filename):
    average_deaths_list_CA = []
    average_deaths_list_US = []
    count = 0
    cur.execute('SELECT Canada.CA_total_deaths, US.US_total_deaths FROM Canada JOIN US ON Canada.date_id = US.date_id')
    cases = cur.fetchall()
    for i in range(10):
        deaths_list_CA = []
        deaths_list_US = []
        for deaths_tup in cases[count:count+10]:
            deaths_list_CA.append(deaths_tup[0])
            deaths_list_US.append(deaths_tup[1])
        deaths_sum_CA, deaths_sum_US = sum(deaths_list_CA), sum(deaths_list_US)
        average_deaths_CA, average_deaths_US = (deaths_sum_CA / 10), (deaths_sum_US / 10)
        average_deaths_list_CA.append(average_deaths_CA)
        average_deaths_list_US.append(average_deaths_US)
        count += 10

    with open(filename, "w", newline="") as outFile:
        csv_writer = csv.writer(outFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL) 
        header = ['Average Total Canada COVID19 Deaths over Ten Period Intervals', 'Average Total U.S. COVID19 Deaths over Ten Period Intervals']
        csv_writer.writerow(header)
        zip_object = zip(average_deaths_list_CA, average_deaths_list_US)
        for average_CA, average_US in zip_object:
            file_line = []
            file_line.append(str(average_CA))
            file_line.append(str(average_US))
            csv_writer.writerow(file_line) 

    return average_deaths_list_CA, average_deaths_list_US

def bar_chart(CA_cases_data, US_cases_data):
    time_periods = ['06/01/20 - 06/10/20', '06/11/20 - 06/20/20', '06/21/20 - 06/30/20', '07/01/20 - 07/10/20', '07/11/20 - 07/20/20', '07/21/20 - 7/30/20', '07/31/20 - 08/09/20', '08/10/20 - 08/19/20', '08/20/20 - 08/29/20', '08/30/20 - 09/08/20']
    n = np.arange(len(time_periods))
    width = 0.25
    fig, ax = plt.subplots()
    CA_bars = ax.bar(n, CA_cases_data, width, label='Canada') 
    US_bars = ax.bar(n + width, US_cases_data, width, label='U.S.')
    ax.set_xticks(n + width / 2)
    plt.xticks(rotation = 53, ha = "right")
    ax.set_xticklabels(time_periods)
    ax.legend()
    ax.set(xlabel = "Ten Day Period", ylabel = "Average Total COVID19 Cases")
    ax.set_title("Average Total COVID19 Cases for Canada and the U.S. over 100 days", loc = "right", pad = 25)
    fig.tight_layout()
    ax.autoscale_view()
    fig.savefig("CA&US_Cases.png")
    plt.show()

def scatter_plot(CA_death_data, US_death_data):
    time_periods = ['06/01/20 - 06/10/20', '06/11/20 - 06/20/20', '06/21/20 - 06/30/20', '07/01/20 - 07/10/20', '07/11/20 - 07/20/20', '07/21/20 - 7/30/20', '07/31/20 - 08/09/20', '08/10/20 - 08/19/20', '08/20/20 - 08/29/20', '08/30/20 - 09/08/20']
    n = np.arange(len(time_periods))
    x = n
    fig= plt.figure()
    width = 1
    axes = fig.add_subplot(1,1,1)
    axes.set_xticks(n + width / 4)
    axes.set_xticklabels(time_periods)
    plt.xticks(rotation = 55, ha = "right")
    y= CA_death_data
    y2= US_death_data
    axes.scatter(x, y, s = 15, c='r', edgecolor='black', label='Canada')
    axes.scatter(x, y2, s =15, c="b", edgecolor='black', label='U.S.')
    axes.set_title("Average Total COVID19 Deaths for Canada and the U.S. over 100 days", loc = "right", pad = 15)
    plt.xlabel("Ten Day Period")
    plt.ylabel("Average Total COVID19 Deaths")
    plt.legend(loc = "upper left")
    fig.tight_layout()
    axes.autoscale_view()
    fig.savefig("CA&US_Deaths.png")
    plt.show()

def main():
    cur, conn = setUpDatabase('Covid.db')
    getDataCanada(conn, cur)
    get_US_Data(conn, cur)
    createDates(conn, cur)

    cur.execute("SELECT COUNT (date_id) FROM Canada")
    Canada_table_length= cur.fetchone()[0]  
    cur.execute("SELECT COUNT (date_id) FROM US")
    US_table_length= cur.fetchone()[0]  

    if (Canada_table_length == 100) and (US_table_length == 100):
        averageCASESCanada, averageCASESUS = average_cases(cur, "Average_Cases_Data.csv")
        averageDEATHSCanada, averageDEATHSSUS = average_deaths(cur, "Average_Deaths_Data.csv")
        bar_chart(averageCASESCanada, averageCASESUS)
        scatter_plot(averageDEATHSCanada, averageDEATHSSUS)

if __name__ == "__main__":
    main()


        
