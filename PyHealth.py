import requests
import json
from PyHealthErrors import UnknownAPIError, InvalidAPICredentials
import pandas as pd

class PyHealth(object):
    '''
    A Python wrapper for the Definitive Healthcare API. Can query hospital info
    and executives for the hospitals. Can get a list of all hospitals or use an
    id to get one of the hospitals at a time.
    '''
    def __init__(self,username,password):
        self.username = username
        self.password = password
    
    def auth(self,proxy=True,http='http://10.10.5.18:8080',https='http://10.10.5.18:8080'):
        '''
        Send a post request to the API to retrieve API key.
        '''
        self.proxy = proxy
        self.http = http
        self.https = https
        payload = {
            "grant_type":"password",
            "username":self.username,
            "password":self.password
        }
        if proxy==True:
            self.proxy_dict = {
            'http'  : http,
            'https' : https
              }
            response = requests.post("https://api.defhc.com/v4/token",data=payload,proxies=self.proxy_dict)
        else:
            response = requests.post("https://api.defhc.com/v4/token",data=payload)
        response = json.loads(response.text)
        try:
            er = response["error"]
            if er == "API Access denied.":
                raise InvalidAPICredentials("Error: Username or Password is not valid.")
            else:
                raise UnknownAPIError("Error: An unknown API error occurred.")
        except KeyError:
            self.token = response['access_token']
    
    def __check_auth(self,response):
        '''
        Checks to see if current API token is calid. If it is, returns True. If
        it is not, re-authenticates API token using same credentials as originally
        provided.
        '''
        response = json.loads(response.text)
        try:
            er = response["error"]
            if er["message"] == "Authorization has been denied for this request.":
                print("API Key expired, re-authenticating...")
                self.auth(self.proxy,self.http,self.https)
            else:
                raise UnknownAPIError("Error: An unknown API error occurred.")
        except KeyError:
            return True
    
    def getHospital(self,id=None):
        '''
        By defaults, returns all available hospitals. If 'id' is not
        set to None, will return hospital with specific id.
        '''
        if id == None:
            while True:
                headers = {"Authorization": "Bearer {token}".format(token=self.token)}
                if self.proxy == True:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals",headers=headers,proxies=self.proxy_dict)
                else:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals",headers=headers)
                check = self.__check_auth(response)
                if check == True:
                    return pd.DataFrame(json.loads(response.text)["value"])
                else:
                    self.auth(self.proxy,self.http,self.https)
                    continue
        else:
            while True:
                headers = {"Authorization": "Bearer {token}".format(token=self.token)}
                if self.proxy == True:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals({})".format(id),headers=headers,proxies=self.proxy_dict)
                else:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals({})".format(id),headers=headers)
                check = self.__check_auth(response)
                if check == True:
                    return pd.DataFrame(json.loads(response.text),index=[0])
                else:
                    self.auth(self.proxy,self.http,self.https)
                    continue
    def getExec(self,id=None):
        '''
        By default, returns all executives for a hospital. If 'id' is not set to None,
        will return hospital with specific id
        '''
        payload = {"$expand":"Executives"}
        if id == None:
            while True:
                headers = {"Authorization": "Bearer {token}".format(token=self.token)}
                if self.proxy == True:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals",params=payload,headers=headers,proxies=self.proxy_dict)
                else:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals",params=payload,headers=headers)
                check = self.__check_auth(response)
                if check == True:
                    response = json.loads(response.text)
                    execs = [exec["Executives"] for exec in response["value"]]
                    exec_list = []
                    for i in execs:
                        for e in i:
                            exec_list.append(e)
                    return pd.DataFrame(exec_list)
                else:
                    self.auth(self.proxy,self.http,self.https)
                    continue
        else:
            while True:
                headers = {"Authorization": "Bearer {token}".format(token=self.token)}
                if self.proxy == True:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals({})".format(id),params=payload,headers=headers,proxies=self.proxy_dict)
                else:
                    response = requests.get("https://api.defhc.com/v4/odata-v4/Hospitals({})".format(id),params=payload,headers=headers)
                check = self.__check_auth(response)
                if check == True:
                    response = json.loads(response.text)["Executives"]
                    return pd.DataFrame(response)
                else:
                    self.auth(self.proxy,self.http,self.https)
                    continue

if __name__ == "__main__":
    pyHealth = PyHealth("email","password")
    pyHealth.auth()
    print(pyHealth.getExec())
                



        
    
