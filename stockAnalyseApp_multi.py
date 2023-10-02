import mysql.connector
from mysql.connector import Error
import pandas as pd
import sqlalchemy
import yfinance as yf
import csv
import datetime as dt

tickers = [
'1AG.AX','A2M.AX','AAC.AX','AAP.AX','AHF.AX','AVG.AX',
'BEE.AX',
'BFC.AX',
'BGA.AX',
'BUB.AX',
'CBO.AX',
'CGC.AX',
'CSF.AX',
'CSS.AX',
'DBF.AX',
'E33.AX',
'ELD.AX',
'ERG.AX',
'FFF.AX',
'FFI.AX',
'FRM.AX',
'FSF.AX',
'GDA.AX',
'HPP.AX',
'HVM.AX',
'ING.AX',
'LGL.AX',
'LRK.AX',
'LV1.AX',
'MBH.AX',
'MCA.AX',
'NOU.AX',
'NUC.AX',
'NZK.AX',
'NZS.AX',
'OJC.AX',
'PFT.AX',
'RFA.AX',
'RIC.AX',
'SFG.AX',
'SGLLV.AX',
'SHV.AX',
'SM1.AX',
'TFL.AX',
'TSI.AX',
'TWE.AX',
'UMG.AX',
'WLD.AX',
'WNR.AX',
'WNX.AX',
'WOA.AX',
'YOW.AX'
]

tickers = [yf.Ticker(ticker) for ticker in tickers]
print(tickers)

dfs = []

### CREATRE FINANCIAL STATEMENT CSV
print("Creating financial statement csv...")
for ticker in tickers:
    pnl = ticker.financials
    bs = ticker.balancesheet
    cf = ticker.cashflow

    fs = pd.concat([pnl, bs, cf])

    data = fs.T
    data = data.reset_index()
    data.columns = ['Date', *data.columns[1:]]
    data['Ticker'] = ticker.ticker
    dfs.append(data)

parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

for df in dfs:
    df.columns = parser._maybe_dedup_names(df.columns)

df = pd.concat(dfs, ignore_index=True)
df = df.set_index(['Ticker','Date'])

df.to_csv('Data/financialStats_multi.csv')
read_stat_csv = pd.read_csv('Data/financialStats_multi.csv')
print(read_stat_csv.head())

### CREATE STOCK HISTORY CSV
print("Creating stock history csv")
dfs = []

now = dt.datetime.now()
start = dt.datetime(now.year -1, now.month , now.month)
end = dt.datetime(now.year , now.month, now.day)

for ticker in tickers:
    hist = ticker.history(interval='1d', start=start,end=end)
    hist = hist.reset_index()
    #data.columns = ['Date', *data.columns[1:]]
    hist['Ticker'] = ticker.ticker
    dfs.append(hist)

parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

for df in dfs:
    df.columns = parser._maybe_dedup_names(df.columns)

df = pd.concat(dfs, ignore_index=True)
df = df.set_index(['Ticker','Date'])

df.to_csv('Data/stockHistory_multi.csv')
read_stockHist_csv = pd.read_csv('Data/stockHistory_multi.csv')
print(read_stockHist_csv.head())

### CREATE STOCK EARNINGS CSV

#print("Creating earnings csv...")
#dfs = []

#for ticker in tickers:
#    earn = ticker.earnings
#    earn = earn.reset_index()
#    #earn.columns = ['Date', *data.columns[1:]]
#    earn['Ticker'] = ticker.ticker
#    dfs.append(earn)

#parser = pd.io.parsers.base_parser.ParserBase({'usecols': None})

#for df in dfs:
#    df.columns = parser._maybe_dedup_names(df.columns)

#df = pd.concat(dfs, ignore_index=True)
#df = df.set_index(['Ticker','Year'])

#df.to_csv('Data/earnings_multi.csv')
#read_earn_csv = pd.read_csv('Data/earnings_multi.csv')
#print(read_earn_csv.head())

### CREATE STOCK INFO CSV
print("Creating stock info csv...")
for ticker in tickers:
    stock_info = ticker.info
    ticker_id = ticker.ticker
#    stock_info = stock_info.reset_index()
    stock_info['Ticker'] = ticker.ticker
#    dfs.append([stock_info])
#    print(stock_info)
    df = pd.DataFrame([stock_info])
    df = df.set_index(['Ticker'])
    df.to_csv('Data/ticker_{}.csv'.format(ticker_id))

li = []

for count,ticker in enumerate(tickers):
    ticker_id = ticker.ticker
    df = pd.read_csv('Data/ticker_{}.csv'.format(ticker_id), index_col=None, header=0, )
#    df = df.drop(columns=['longBusinessSummary'])
    #df.insert(0,'Ticker',ticker_id, False)
    li.append(df)

frame = pd.concat(li, axis=0, ignore_index=True )
print(frame)

frame.to_csv('Data/joined_stockInfo.csv', index=False)
read_stockInfo_csv = pd.read_csv('Data/joined_stockInfo.csv')
print(read_stockInfo_csv)

### CONNECT TO DATABASE
try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        passwd="P@ssw0rd135",
        database="testdatabase",
        use_pure=True
        )

    if db.is_connected():
        cursor = db.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)

### INSERT financialStats_multi DATA
        #print("Table is created....")
        #loop through the data frame
        #cursor.execute("DROP TABLE financialStats_multi")
        #db.commit()
        #print("Try inserting financial statement records")

        with open("Data/financialStats_multi.csv", "r") as file:
          reader = csv.reader(file)
          #print(reader)
          # Get the column names from the first row
          columns = next(reader)
          print(columns)
          # Loop through the columns and remove white spaces
          for i, column in enumerate(columns):
              columns[i] = column.replace(' ', '_')
          # Create the table
          #cursor.execute(f"CREATE TABLE IF NOT EXISTS financialStats_multi ({','.join(['`' + column + '` BLOB' for column in columns])})")
          #db.commit()
          #print(cursor)
          # Insert the rows
          for row in reader:
            cursor.execute(f"INSERT INTO financialStats_multi VALUES ({','.join(['%s' for _ in range(len(columns))])})", row)
            #print(cursor)
        db.commit()

        print("Financial statement records Commited!")

### INSERT stockHistory_multi  DATA
        #loop through the data frame
        cursor.execute("DROP TABLE stockHistory_multi")
        db.commit()
        print("Try inserting stock histroy records")

        with open("Data/stockHistory_multi.csv", "r") as file:
          reader = csv.reader(file)
          print(reader)
          # Get the column names from the first row
          columns = next(reader)
          print(columns)
          # Create the table
          cursor.execute(f"CREATE TABLE IF NOT EXISTS stockHistory_multi ({','.join(['`' + column + '` TEXT' for column in columns])})")
          print(cursor)
          # Insert the rows
          for row in reader:
            cursor.execute(f"INSERT INTO stockHistory_multi VALUES ({','.join(['%s' for _ in range(len(columns))])})", row)
            print(cursor)
        db.commit()

        print("Stock history records Commited!")

### INSERT joined_stockInfo DATA
        #loop through the data frame
        print("Try inserting stock info records")
        cursor.execute("DROP TABLE joined_stockInfo")
        db.commit()

        with open("Data/joined_stockInfo.csv", "r") as file:
          reader = csv.reader(file)
          print(reader)
          # Get the column names from the first row
          columns = next(reader)
          print(columns)
          # Create the table
          cursor.execute(f"CREATE TABLE IF NOT EXISTS joined_stockInfo ({','.join(['`' + column + '` TEXT' for column in columns])})")
          print(cursor)
          # Insert the rows
          for row in reader:
            cursor.execute(f"INSERT INTO joined_stockInfo VALUES ({','.join(['%s' for _ in range(len(columns))])})", row)
            print(cursor)
        db.commit()

        print("stock info records Commited!")

    ### CLOSE DATABASE CONNECTION
    db.close
    print("close database connection")
except Error as e:
            print("Error while connecting to MySQL", e)
