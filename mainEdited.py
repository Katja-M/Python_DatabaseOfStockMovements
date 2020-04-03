#Objective: Create a searchable database of all daily stock price movements

import urllib.request
import http.cookiejar #defines classes for automatic handling of HTTP cookies
import zipfile
import os
import time #various functions to manipulate time values #helpful when tryiing to format time from files
import sqlite3 #runs on local machine, requires no installation or set up # no pip install needed
from datetime import datetime #helpful when tryiing to format time from files
import csv
import xlsxwriter # in order to generate an excel spreadsheet

###################################################################
# All functions are on top
###################################################################

#Functions to download and unzip file

def download(localZipFilePath, urlofFileName):
    #localZipFilePath is a variable with the local file in which to store the file. This is a path on the local desktop
    #urlofFileName is the variable with the URL to download

    #The following code is a boilerplate to deal with the fact that the website blocks bots/automated scripts
    hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
        }

    #The following code will actually download and store the file

    #Make the web request by using a web brower
    webRequest = urllib.request.Request(urlofFileName, headers = hdr)

    try:
        #Making download request
        page = urllib.request.urlopen(webRequest)
        #save the content of this opage into a variable called 'content'
        #By reading the content of the downloaded page we put the content into the variable 'content'
        content = page.read()
        #Save content to local disk
        #Use open command to create a new file
        output = open(localZipFilePath, 'wb')
            #'w'indicates that we intend to write
            #'b' indicats that this is a binary file, i.e. not a text file
        #Writing content to the file
        #bytearray() returns an array of bytes of the size of content
        #The bytearray can then be written to file
        output.write(bytearray(content))
        #File needs to be closed before moving on
        output.close()
    except urllib.error.HTTPError as httpe:
        print(httpe.fp.read())
        print('Looks like to file download did not go through for url = ' + urlOfFileName)

def unzip(localZipFilePath, localExtractFilePath):
    #localExtractFilePath is the variable with the local directory in which to extract the zip file above

    #Check if the zip file was downloaded successfully
    if os.path.exists(localZipFilePath):
        print("Cool! " + localZipFilePath + ' exists...proceeding')

        # We do not know how many files are in the zip file
        #Hence, initialize array in which to save the names of the files
        listOfFiles = []
        #Open the zip file
        fh = open(localZipFilePath, 'rb')
            # r indicates that file is open in read mode
            # b indicates that this is a binary file

        #binary files require handlers (unlike text files)
        #the zipfile handler knows how to read the list of zipped up files
        zipFileHandler = zipfile.ZipFile(fh)

        #iterating over list in a for-loop
        for filename in zipFileHandler.namelist():
            #Files are extracted
            zipFileHandler.extract(filename, localExtractFilePath)
            #Add to the list of files we have extracted
            listOfFiles.append(localExtractFilePath + filename)
            print("Extracted " + filename + " from the zip file to " + (localExtractFilePath + filename))

        print("In total, we extracted ", str(len(listOfFiles)), ' files.')

        #Close the file
        fh.close()

#The following function builds on top on these two functions
#Given the list of months and the list of years it will fetch all the zip-files for the months and years.
def downloadAndUnzipForPeriod(listOfMonths, listOfyears):
    for year in listOfyears:
        for month in listOfMonths:
            for dayOfMonth in range(2):
                #Since range provides list with numbers from 0 - 30, we add one to each day
                date = dayOfMonth + 1
                #The following code constructs a URL for one particular day.
                #A typical URL: "https://archives.nseindia.com/content/historical/EQUITIES/2020/MAR/cm26MAR2020bhav.csv.zip"

                #Convert a number to a string
                dateStr = str(date)
                #In case the day is less then 10, we need to put a zero in front of the number
                if date < 10:
                    dateStr = '0'+dateStr
                #This print statement just allows to user to know where we are in the download process
                print(dateStr, '-', month,'-',year)

                #Construct the filename
                fileName ='cm' + str(dateStr) + str(month) + str(year) + "bhav.csv.zip"
                #Construct the entire URL
                urlofFileName = 'https://www1.nseindia.com/content/historical/EQUITIES/' + year + '/' + month + '/' + fileName
                #https://www1.nseindia.com/content/historical/EQUITIES/2018/JAN/cm01JAN2018bhav.csv.zip
                #Construct the file on our local hard disk where we wish to save the downloaded file
                #The folder Data needs already be created
                localZipFilePath = 'C:/Users/Katja/PythonCodingTraining/DatabaseOfStockMovements/Data/' + fileName

                #Make the call to download the function
                download(localZipFilePath,urlofFileName)
                #Make the call to the unzip function
                unzip(localZipFilePath,localExtractFilePath)
                #We will pause 10 seconds to not overload the NSE website
                time.sleep(10)
    print('Done with downloading and extracting')

#Function to parse the files

#The following function will read all the rows in the csv. file and
# insert these rows in the table in our database
def insertRows(fileName, conn):
    c = conn.cursor()
    # The linenum variable keeps track of the line we are reading
    lineNum = 0 
    #The file needs to be open in r, NEVER rb mode.
    with open(fileName, 'r') as csvfile:
        #Let's get a handler to deal correctly with the csvfile
        lineReader = csv.reader(csvfile, delimiter = ',',quotechar = '\'')

        #Iterate of these lines by using a for loop
        for row in lineReader:
            lineNum = lineNum + 1
            #If it is the first line, we want to skip the line because it is the header
            if lineNum == 1:
                print('Header row, skipping')
                continue
            #Insert a row of data in our database table
            #We want to format the timestamp in a particular way
            date_object = datetime.strptime(row[10], '%d-%b-%Y')

            #We will insert the data into the database one row at a time.
            #The data must correspond in the number and type to the column headers of the table
            #The following list oneTuple has the exact same schema that we specified for our table
            #Strings in the csv.-file are converted to floats
            oneTuple = [row[0], row[1],float(row[2]), float(row[3]),float(row[4]), float(row[5]), float(row[6]), float(row[7]),float(row[8]), float(row[9]), date_object, float(row[11]), row[12]]
            #The following statement will actually insert a single row into the table called prices
            #We are putting in placeholders for the tuple that is going to be inserted.
            c.execute("INSERT INTO prices VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", oneTuple)
        
    #We just wrote to the database, so we must commit our writes
    conn.commit()
    print('Done iterating over file content. - The file has been closed.')

#Functions to create Excel-file
# The following function will take in a ticker and a database connection and it will return an excel file
def createExcelWithDailyPriceMoves(ticker, conn):
    #We will use the cursor to execute SQL commands
    c = conn.cursor()
    cursor = c.execute('SELECT symbol, timestamp, close FROM prices WHERE symbol = ? and series = ? ORDER BY timestamp', (ticker, series))

    excelFileName = 'C:/Users/Katja/PythonCodingTraining/DatabaseOfStockMovements/' + ticker + '.xlsx'
    #Creating a workbook. In this case, opening and creating are the same because the file does not yet exist
    workbook = xlsxwriter.Workbook(excelFileName)
    #Create an empty worksheet in this workbook, named 'summary'
    worksheet = workbook.add_worksheet('Summary')
    #The list of values will be written one per cell, starting from the cell address we specify
    worksheet.write_row('A1',['Top Traded Stocks'])
    #Second row has coloumn headers
    worksheet.write_row('A2', ['Stock', 'Date','Closing'])
    lineNum = 3
    # The result of the query is a list. Hence, we can iterate over this list using a for loop
    for row in cursor:
        worksheet.write_row('A'+str(lineNum), list(row))
        print('A'+str(lineNum), list(row))
        lineNum = lineNum + 1
    #We create a line chart in excel
    chart1 = workbook.add_chart({'type':'line'})
    #Configure the first series
    chart1.add_series({
        'categories':'=Summary!$B$3:$B$' + str(lineNum), #lineNum represents now the last row of our data
        'values': '=Summary!$C$3:$C$' + str(lineNum)
    })
    # To format the chart, we add a chart title and some axis labels
    chart1.set_title({'name':ticker})
    chart1.set_x_axis({'name': 'Date'})
    chart1.set_y_axis({'name': 'Closing Price'})
    
    #Insert the chart into the worksheet with an offset
    worksheet.insert_chart('F2', chart1,{'x_offset' : 25, 'y_offset' : 10})
    workbook.close()


################################################################
# Step 1: Create a table in our database to hold the stock data
################################################################

#Connecting to a database
#Open a connection to a database which basically represents the database
#Name of the database 'example'. By default, the database lives on disk.
conn = sqlite3.connect('exampleedited.db')
#Set up a cursor.
#The cursor allows to run commands on the database.
cursor = conn.cursor()
#Create table with an execute statement. The SQL-statement is written inside the brackets.
#real to specify decimal numbers.
cursor.execute('CREATE TABLE prices (SYMBOL text, SERIES text, OPEN real, HIGH real, LOW real, CLOSE real, LAST real, PREVCLOSE real, TOTTRDQTY real, TOTTRDVal real, TIMESTAMP date, TOTALTRADES real, ISIN text, PRIMARY KEY (SYMBOL, SERIES, TIMESTAMP)  )')
#Creating a table is a write operation, which means we have to commit before moving on.
conn.commit()

##################################################################################
# Step 2: Download a bunch of files and unzip them
##################################################################################

#Initialize a variable with a local directory in which to extract the zip file above
localExtractFilePath = 'C:/Users/Katja/PythonCodingTraining/DatabaseOfStockMovements/Data/'

#Initialize list of month and use the Indian Stock Exchange Pattern

listOfMonths = ['APR']
listOfYears = ['2019']

downloadAndUnzipForPeriod(listOfMonths, listOfYears)

#############################################################################
# Step 3: Parce each file, and insert each row of each file into the database
##############################################################################

#Use os-module to get a list of all files in a particular directory
for file in os.listdir(localExtractFilePath):
    #We iterate over each file and for each file we call the insertRow function
    if file.endswith('.csv'):
        insertRows(localExtractFilePath+'/'+file, conn)

#########################################################################################
# Step 4: Run a test query against the database to make sure it is set up OK
#########################################################################################

ticker = 'ICICIBANK'
series = 'EQ'
#c = conn.cursor()
cursor_query = cursor.execute('SELECT symbol, max(close), min(close), max(timestamp),min(timestamp), count(timestamp) FROM prices WHERE symbol = ? and series = ? GROUP BY symbol ORDER BY timestamp', (ticker,series))
for row in cursor_query:
    print(row)

#####################################################################################
# Step 5: Run a query to get all data for a given stock, and create an excel summary
#####################################################################################

#Create a connection
#conn = sqlite3.connect('example.db')
#Call the function above to create an excel file for the ticker BEDMUTHA
createExcelWithDailyPriceMoves('BEDMUTHA',conn)
#No need to commit because we only read from the database

#################################################################
# Step 6: Dropping the table to clear up
#################################################################
"""
# Step 1: Open a connection
conn = sqlite3.connect('example.db')
# Step 2: Get a cursor
c = conn.cursor()
#Step 3: Drop the table so we leave the database in the same state in which we started with
c.execute('DROP TABLE prices')
#Step 4: Commit changes because we wrote to the database
conn.commit()
#Step 5: Close the connection
conn.close()
"""