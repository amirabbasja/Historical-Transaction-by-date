from symtable import Symbol
import pandas as pd
from binance import Client
import re
import os
import pickle
import time
from datetime import datetime

from tenacity import retry_if_exception

# To this date (4/16/2022) binance only provides historical trades data 
# with to "trade id". And currently (To the authors knowledge) there
# is no direct method of acquiring the historical trades data with date.
# In this script we use bisection method to as a method to search for the
# trades that are between two dates. For the better performance, first, 
# "historicalTradesRageFinder" will return a range that contains our 
# desired "tradeId". period containing the target trade id will be returned.
# Some time, we may have multiple transactions happening in a single timestamp
# Also there may be a time that there is no trade in the timestamp. again, in
# these cases we will return the most recent tradeId.
# in these cases we return the first tardeId that has happened
# Note 1: That the length of the period can be altered by user 
# Note 2: The timestamp is in milliseconds
# Note 3: Tollerance's unit time (ms), not tradeId

# Version 1: support for geting tradeId via online connection
# Version 2: support for geting tradeId via offline connection
 

def historicalTradIdByDate(symbol:str, targetTime:int, tollerance:int, client = None, path = None, verbouse = True, online = True):
    # Note that if online == True, client has to be passed to the method else
    # path has to be passed.

    if online:
        # Using online connection for getting tradeId

        # Getting the most recent trade id
        temp = client.get_historical_trades(symbol = SYMBOL, limit = 1)
        latestId = temp[0]["id"]
        latestTime = temp[0]["time"]

        # TO increase the search speed, we should not start from the latest 
        # tradeId as the upperBound. we can approximate the target tradeId 
        # but sometimes this may cause some implications (when approximated 
        # trade time is less than target time). To avoid this implications
        # we use the latest trade as the upperbound but if you want to use
        # Approximation, you can uncomment the lines below
        # Now we calculate whats the average time necessary for 100,000 trades
        # This value is in milliseconds
        # temp = client.get_historical_trades(symbol = SYMBOL, limit = 1, fromId = latestId - 100000)[0]["time"]
        # aveTimeForTrade = (latestTime - temp) // 100000

        # # By having the average time for each transaction, we can approximate
        # # the "tradeId" to start the search from
        # searchStartId = latestId - (latestTime - targetTime) // aveTimeForTrade
        searchStartId = client.get_historical_trades(symbol = SYMBOL, limit = 1)[0]["id"]

        # Start the binary search (Bisection method)
        lowerBoundId = 0 # Will be calculated in the first iteration of search loop
        lowerBoundTime = 0
        upperBoundId = searchStartId
        upperBoundTime = client.get_historical_trades(symbol = SYMBOL, limit = 1, fromId = upperBoundId)[0]["time"]

        # Upperbound has to be bigger than targetTime, else increase the upperbound
        # with amount of 1000000 trades in each iteration 
        # while(upperBoundTime <= targetTime):
        #     print(targetTime - upperBoundTime)
        #     upperBoundId = int(upperBoundId + 1000000)
        #     upperBoundTime = client.get_historical_trades(symbol = SYMBOL, limit = 1, fromId = upperBoundId)[0]["time"]

        # At the start of the search, the upper boundary is know but the
        # lower boundary is unknown, thus in this loop we locate the lower bound
        if verbouse : print("Searching for lower boundary...")

        j = 1

        while(True):
            # The step size is 1e6; Change it if necessary
            temp = client.get_historical_trades(symbol = SYMBOL, limit = 1, fromId = int(searchStartId - j * 1e7))
            
            if(temp[0]["time"] <= targetTime):
                # The lower bound is found
                start = False
                lowerBoundId = temp[0]["id"]
                lowerBoundTime = temp[0]["time"]
                if verbouse : print(f"The lower boundary time found! lb time = {lowerBoundTime} || ub time = {upperBoundTime} || target time = {targetTime}")
                break
            else:
                j += 1
            time.sleep(.1)

        if verbouse : print("starting the bisection...")

        while(True):
            midId = (upperBoundId + lowerBoundId) // 2
            temp = client.get_historical_trades(symbol = SYMBOL, limit = 1, fromId = midId)
            midTime = temp[0]["time"]


            if(lowerBoundTime <= targetTime and targetTime < midTime):
                upperBoundId = midId 
                upperBoundTime = midTime
            elif(midTime <= targetTime and targetTime <= upperBoundTime):
                lowerBoundId = midId 
                lowerBoundTime = midTime
            

            if(not(tollerance <= upperBoundTime - lowerBoundTime)):
                if verbouse : print("Bisection is done!")
                break
            if verbouse : print(f"required tollerance: {tollerance} || current tollerance: {upperBoundTime - lowerBoundTime}({round(tollerance/(upperBoundTime - lowerBoundTime)*100,0)}%)")

            # We do rest requests so its only logical to implement a small delay so 
            # we wont be blocked by the server
            time.sleep(.1)
        if 1000 < upperBoundId - lowerBoundId + 1:
            print("Decrease the tollerance")
            return 0

        if verbouse : print("Approximate range found. getting the exact tradeId...")

        temp = client.get_historical_trades(symbol = SYMBOL, limit = upperBoundId - lowerBoundId + 1, fromId = lowerBoundId)

        if(1< len(temp)):
            for i in range(len(temp)):
                if i == 0:
                    pass
                if(temp[i]["time"] == targetTime):
                    return temp[i]["id"]

                if (int(temp[i-1]["time"]) - targetTime) < 0 and 0 < (int(temp[i]["time"]) - targetTime):
                    return temp[i]["id"]
        elif(len(temp) == 1):
            return temp[0]["id"]            
        else:
            print("Some thing is wrong!")
            return 0
    else:
        # Using offline connection for getting the tradeId path is targeting the 
        # folder that is containing the historical trades. The file names has to
        # be inthe following format: HistoricalTrades_BTCUSDT_{endId}____{startId}.pickle
        # Note that for increasing the application's performance, we import pickle
        # objects, instead of reading excel files 
        files = os.listdir(path)
        pattern = r"HistoricalTrades_BTCUSDT_(\d+)____(\d+).pickle"
        endTime = 0; startTime = 0

        for file in files:
            if(file.endswith(".pickle")):          
                df = pd.read_pickle(os.path.abspath(PATH)+"//"+file )
                startTime = df["time"].iloc[0]
                endTime = df["time"].iloc[-1]
                
                # print(f"{startTime} ({targetTime - startTime}) {targetTime} ({endTime-targetTime}) {endTime}")

                if (startTime <= targetTime and targetTime <= endTime):
                    nearestIndex = (df["time"]-targetTime).abs().idxmin()
                    return df["id"].iloc[nearestIndex]
            

            


client = Client(api_key = "gcJLtOs6tZYTOBEyIYY0JZBDagmFMkc5SY5T8R792cCUgBn7YCLfG7pOJZG3J36x")

#Defining the necessary variables
SYMBOL  = "BTCUSDT"
PATH = "../../Data/HistoricalTrades/"
targetTime = 1650768807449
tollerance = 1000 # time milliseconds

# id = historicalTradIdByDate(SYMBOL, client, targetTime, tollerance, verbouse = True)
# print(f"ID: {id}")
id = historicalTradIdByDate(symbol = SYMBOL, targetTime = targetTime, tollerance = tollerance, online = False, path = PATH)
print(id)