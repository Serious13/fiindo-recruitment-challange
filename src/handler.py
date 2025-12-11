from urllib import request
from auth import auth
from dotenv import load_dotenv
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools
from types import SimpleNamespace
import pandas as pd
from datetime import datetime
import sqlite3

load_dotenv()

class HandleFiindo:

    healthUrl = "https://api.test.fiindo.com/health"
    dbHealthUrl = "https://api.test.fiindo.com/db_health"
    symbolsUrl = "https://api.test.fiindo.com/api/v1/symbols"
    generalSymbolUrl = "https://api.test.fiindo.com/api/v1/symbol"
    finnReportUrl = "https://api.test.fiindo.com/api/v1/financials"
    stockUrl = "https://api.test.fiindo.com/api/v1/eod"
    symbolDataUrl = "https://api.test.fiindo.com/api/v1/general/"
    itemsArr = []

    bank = []
    software = []
    electronics = []  
    results = []

    def __init__(self, url):
        self.first_name = os.getenv("FIRST_NAME")
        self.last_name = os.getenv("LAST_NAME")
       
    def getSymbols(self)-> object:
        try:
            symbols = auth(self.first_name, self.last_name).fetchData(self.symbolsUrl)
            return symbols
        except Exception as e:
            return str(e)

    def getHealth(self)-> object:
        try:
            health = auth(self.first_name, self.last_name).fetchData(self.healthUrl)
            return health
        except Exception as e:
            return str(e)
    
    def getSymbolData(self, symbol)-> object:        
        bankUrl = self.symbolDataUrl + symbol
        try:
            response = auth(self.first_name, self.last_name).fetchData(bankUrl)
            return response
        except Exception as e:
            return str(e)
     
    def getDbHealth(self)-> object:
        try:
            dbHealth = auth(self.first_name, self.last_name).fetchData(self.dbHealthUrl)        
            return dbHealth
        except Exception as e:
            return str(e)

    def getFinnReport(self, symbol : str, statement : str)-> object:
        try:
            finnReport = auth(self.first_name, self.last_name).fetchData(f'{self.finnReportUrl}/{symbol}/{statement}')       
            return finnReport
        except Exception as e:
            return str(e)

    def getStockPrice(self, symbol : str):
        try:
            finnReport = auth(self.first_name, self.last_name).fetchData(f'{self.stockUrl}/{symbol}')       
            return finnReport
        except Exception as e:
            return str(e)

    def findSymbolExchangeCodes(self, industry : str):
        tickerData = []
        bankData = {}
        tickerCodeExch = ""
        with open(f'{industry}.json') as f:
            bankData = json.load(f)
            #bankData = [Item(x) for x in bankData]
            data_dict = { i: item for i, item in enumerate(bankData) }
            if isinstance(data_dict, dict):
                print("Top-level is a dictionary")
                for key, value in data_dict.items():
                    if not (isinstance(value, str)):
                        print(value["code"] + value["exchange"])
                        tickerCodeExch = value["code"] +"." + value["exchange"]
                        tickerData.append(tickerCodeExch)
            return tickerData
           
    def findPE(self, industry : str):
        eps = -1
        sharePrice = 0
        peRatio = 0
        codes = self.findSymbolExchangeCodes(industry)
        res = {}
        for code in codes:
            income_statement = self.getFinnReport(code, "income_statement")
            if (isinstance(income_statement, dict) and ("fundamentals" in income_statement)):
                reportData = income_statement["fundamentals"]["financials"]["income_statement"]["data"]
                eps = reportData[len(reportData) -1]["eps"]
                stock_price = self.getStockPrice(code)
                df = pd.DataFrame(stock_price['stockprice']['data'])              
                df['date'] = pd.to_datetime(df['date'])
                latest_date = df['date'].max()
                quarter = (latest_date.month - 1) // 3 + 1               
                quarter_start_month = {1: 1, 2: 4, 3: 7, 4: 10}[quarter]
                quarter_start = datetime(latest_date.year, quarter_start_month, 1)
                mask = (df['date'] >= quarter_start) & (df['date'] <= latest_date)
                last_quarter = df.loc[mask]
                avg_price = last_quarter['close'].mean()
                peRatio = avg_price / eps
                res[code] = peRatio
        if (industry == "bank"):
            with open(f'Pe-Bank.json', "w") as f:
                json.dump(res, f, indent=4)  
        if (industry == "software"):
            with open(f'Pe-Software.json', "w") as f:
                json.dump(res, f, indent=4)  
        if (industry == "electronics"):
            with open(f'Pe-Electronics.json', "w") as f:
                json.dump(res, f, indent=4)
        return res 

    def findRevenueGrowth(self): 
        eps = -1
        sharePrice = 0
        peRatio = 0
        revenueGrowth = 0
        revenueQ1 = 0 
        revenueQ2 = 0 
        codes = self.findSymbolExchangeCodes("bank")
        res = {}
        for code in codes:
            with open("isBankMain.json") as f:
                data = json.load(f)
                data  = { i: item for i, item in enumerate(data) }
            print("data", data)
            for key, elem in data.items():
                print("key", elem["id"])
                if (elem["id"] == code):
                    income_statement = elem
            if (isinstance(income_statement, dict)):
                reportData = income_statement["fundamentals"]["financials"]["income_statement"]["data"]
                revenueQ2 = reportData[len(reportData) -1]["revenue"]
                revenueQ1 = reportData[len(reportData) -2]["revenue"]
                revenueGrowth = (revenueQ2 - revenueQ1) / revenueQ1
                res[code] = revenueGrowth
        with open(f'RG-Bank.json', "w") as f:
            json.dump(res, f, indent=4)  

    def findNetIncomeTTM(self, industry : str):
        codes = self.findSymbolExchangeCodes(industry)
        res = {}
        netIncomeTTM = 0
        for code in codes:
            income_statement = self.getFinnReport(code, "income_statement")
            if (isinstance(income_statement, dict)):
                reportData = income_statement["fundamentals"]["financials"]["income_statement"]["data"]
                reportData = reportData[-12:]
                for data in reportData:
                    netIncomeTTM += data["netIncome"]       
                res[code] = netIncomeTTM
        return res        
    # start concurrent requests to fetch all data with given symbols from API /api/v1/symbols
    def startConccurrentQueries(self):
        results = []        
        with ThreadPoolExecutor(max_workers=80) as executor:
            futures = [executor.submit(self.getFinnReport, code, "income_statement") for code in self.findSymbolExchangeCodes("electronics")]
            for f in as_completed(futures):
                results.append(f.result())
                print("results", results)
        with open("isElectronicsMain.json", "w") as f:
            json.dump(results, f, indent=4)  

    def assignCompanies(self, fileName : str):
        with open(f'{fileName}.json') as f:
            companies = json.load(f)            
            for company in companies:
                print("company", company)
                if (isinstance(company, dict)):
                    companyData = company["fundamentals"]["profile"]["data"]
                #print("profileData", profileData)
                for data in companyData:
                    industry = data["industry"]
                    print("industry", industry)             
                    if (industry == "Banks - Diversified"):
                        self.bank.append(company)
                    if (industry == "Software - Application"):
                        self.software.append(company)
                    if (industry == "Consumer Electronics"):
                        self.electronics.append(company)
            self.writeToFile("bank",self.bank)
            self.writeToFile("software",self.software)
            self.writeToFile("electronics",self.electronics)
            return companies

    def writeToFile(self, industry : str, data : object):        
        with open(f'{industry}.json', "w") as f:
            json.dump(data, f, indent=4)  



res = HandleFiindo("https://api.test.fiindo.com/")
#print(res.findPE("bank"))
industries = ["bank", "software","electronics"]
bankPERatio = []
softwarePERatio = []
electronicsPERatio = []
#res.findRevenueGrowth()

with open("Pe-software.json") as f:
    data = json.load(f)


conn = sqlite3.connect("fiindo_challenge.db")
cursor = conn.cursor()


cursor.execute(f"""
CREATE TABLE IF NOT EXISTS pe_software_final (
    "id" TEXT PRIMARY KEY,
    "value" TEXT
)
""")

for key, value in data.items():
    print(key,":", value)    
    cursor.execute( "INSERT OR REPLACE INTO pe_software_final (id, value) VALUES (?, ?)",
    (key, value))
conn.commit()
conn.close()

print("JSON inserted into SQLite successfully!")