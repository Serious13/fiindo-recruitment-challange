import requests

class auth:

    def __init__(self, first_name, last_name):
        self.first_name = first_name
        self.last_name = last_name
        self.headers = {
            "Authorization": f"Bearer {self.first_name}.{self.last_name}"
        }
    def fetchData(self, url : str) -> object:
        response = requests.get(url, headers = self.headers)
        print("response", response.json)
        return response.json()
    def fetchDataWithParams(self, url : str, params) -> object:
        params = {
            "industry" : "Banks - Diversified"
        }
        response = requests.get(url, headers = self.headers, params = params)
        print("response", response)
        return response
#    def getHealth(self) -> object:        
#        response = requests.get(healthUrl, headers = self.headers)
#        print("response", response.json)
#        return response.json()

#    def getAllSymbols(self, url) -> object:        
#        response = requests.get(url, headers = self.headers)
#        print("response", response)
#        return response.json()

#    def getBanks(self) -> object:      
#        response = requests.get(url, headers = self.headers)
#        print("response", response)
#        return response.json()    
        
       

