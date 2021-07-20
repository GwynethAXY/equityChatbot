from random import randint, randrange
import random
import json

salaryNumber = [2000000, 4000000]
numberVerLarge = [55000, 80000]
numberLarge = [2700, 4500]
numberMedium = [2200, 3500]
numberSmall = [1000, 2500]
regionNames = ["ANZ", "Americas", "AEJ", "Europe", "Japan"]
percentageHigh = [65, 95]
percentageMedium = [45, 75]
percentageSmall = [30, 45]
number10 = [4, 10]
ratio = [0.8, 1.6]
currencies = ["GBP", "SAR", "USD", "AED", "CNY", "HKD", "AUD"]
sustainabilitySector = ["Agriculture", "Electronics", "Textile"]

numericFormat = '{{ "_id": {{ "$oid": "{}" }}, "currencyAlias": "PRICING", "entityId": {{"$numberInt": "{}"}}, "keyword": "{}", "currencyIso": "{}", "rawValue": {{"numberDouble": "{}"}}, "value": "{}", "valueType": "NUMERIC" }}'

textFormat = '{{ "_id": {{ "$oid": "{}" }}, "currencyAlias": "PRICING", "entityId": {{"$numberInt": "{}"}}, "keyword": "{}", "rawValue": "{}", "value": "{}", "valueType": "TEXT" }}'

def getDummyData(a):
    a = int(a)
    if(a == 1):
        return randrange(numberLarge[0], numberLarge[1], 10)
    elif(a == 2):
        return randrange(numberMedium[0], numberMedium[1], 10)
    elif(a == 3):
        return randrange(numberSmall[0], numberSmall[1], 10)
    elif(a == 4):
        return randrange(percentageHigh[0], percentageHigh[1])
    elif(a == 5):
        return randint(percentageMedium[0], percentageMedium[1])
    elif(a == 6):
        return randint(percentageSmall[0], percentageSmall[1])
    elif(a == 7):
        return randint(number10[0], number10[1])
    elif(a == 8):
        return random.uniform(ratio[0], ratio[1])
    elif(a == 9):
        return random.choice(regionNames)
    elif(a == 10):
        return random.choice(currencies)
    elif(a == 11):
        return randrange(numberVerLarge[0], numberVerLarge[1], 1000)
    elif(a == 12):
        return randrange(salaryNumber[0], salaryNumber[1], 200000)
    else:
        return random.choice(sustainabilitySector)

entityId = 2000001
jsonData= "[\n"

for entityId in range(2000001, 2000001+4305):
    with open('keywordMap.json') as data_file:    
        data = json.load(data_file)
        currencyIso = getDummyData("10")
        for row in data:
            if(row["dataId"] == "9" or row["dataId"] == "10" or row["dataId"] == "x"):
                dummyData = getDummyData(row["dataId"])
                data = textFormat.format(row["oid"], entityId, row["key"], dummyData, dummyData)
            else:
                dummyData = getDummyData(row["dataId"])
                data = numericFormat.format(row["oid"], entityId, row["key"], currencyIso, dummyData, dummyData)

            jsonData += data + ",\n"

jsonData = jsonData[:-2]
jsonData += "]"

f = open("CompanyKeywordsData.json", "w")
f.write(jsonData)
f.close()

        




    


