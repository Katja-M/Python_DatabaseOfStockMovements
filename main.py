#Objective: Create a searchable database of all daily stock price movements

# Step 1: Create a table in our database to hold the stock data

import urllib.request
import http.cookiejar #defines classes for automatic handling of HTTP cookies
import zipfile
import os
import time #various functions to manipulate time values #helpful when tryiing to format time from files
import sqlite3 #runs on local machine, requires no installation or set up # no pip install needed
from datetime import datetime #helpful when tryiing to format time from files

#Connecting to a database
#Open a connection to a database which basically represents the database
#Name of the database 'example'. By default, the database lives on disk.
conn = sqlite3.connect('example.db')
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
def download(PlocalZipFilePath, PurlofFileName):
    #initialize variable with the URL to download
    #Website: National Stock Exchange of India
    urlOfFileName = PurlofFileName

    #initialize a variable with the local file in which to store the file
    #This is a path on the local desktop
    localZipFilePath = PlocalZipFilePath

    #The following code is a boilerplate to deal with the fact that the website blocks bots/automated scripts
    hdr = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
        }

    #The following code will actually download and store the file

    #Make the web request by using a web brower
    webRequest = urllib.request.Request(urlOfFileName, headers = hdr)

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

def unzip(PlocalZipFilePath, PlocalExtractFilePath):
    #initiliaze a variable with the local directory in which to extract
    #the zip file above
    localExtractFilePath = PlocalExtractFilePath

    #Check if the zip file was downloaded successfully
    if os.path.exists(PlocalZipFilePath):
        print("Cool! " + PlocalZipFilePath + ' exists...proceeding')

        # We do not know how many files are in the zip file
        #Hence, initialize array in which to save the names of the files
        listOfFiles = []
        #Open the zip file
        fh = open(PlocalZipFilePath, 'rb')
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
            for dayOfMonth in range(31):
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
                urlofFileName = 'https://archives.nseindia.com/content/historical/EQUITIES/' + year + '/' + 'month' + '/' + fileName
                #Construct the file on our local hard disk where we wish to save the downloaded file
                localZipFilePath = 'C:/Users/Katja/PythonCodingTraining/DatabaseOfStockMovements' + fileName

                #Make the call to download the function
                download(localZipFilePath,urlofFileName)
                #Make the call to the unzip function
                unzip(localZipFilePath,localExtractFilePath)
                #We will pause 10 seconds to not overload the NSE website
                time.sleep(10)
    print('Done with downloading and extracting')

#Initialize a variable with a local directory in which to extract the zip file above
localExtractFilePath = 'C:\Users\Katja\PythonCodingTraining\DatabaseOfStockMovements'

#Initialize list of month and use the Indian Stock Exchange Pattern

listOfMonths = ['JAN', 'FEB','MAR', 'APR', 'MAY', 'JUN','JUL','AUG','SEP','OCT','NOV','DEC']
listOfYears = ['2017','2018']

downloadAndUnzipForPeriod(listOfMonths, listOfYears)