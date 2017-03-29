import csv
import os.path

responseDictionary = {}

# Login with username
username = input("Please enter username: ")
while username == "":
    username = input("Error: username can't be blank\nPlease enter username: ")

paragraphFilePath = "data/paragraphs.csv"
responseFilePath = "data/responses_" + username + ".csv"


# create the response file if it doesn't exist
if os.path.isfile(responseFilePath):
    with open(responseFilePath, "r") as responseFile:
        responseReader = csv.DictReader(responseFile)
        for row in responseReader:
            responseDictionary[row['number']] = row['response']
        print(responseDictionary)
else:
    with(open(responseFilePath, "w+")) as responseFile:
        responseFile.write("number,response\n")
    responseFile.close()

with open(paragraphFilePath) as paragraphFile, open(responseFilePath, "a", newline="") as responseFile:
    paragraphReader = csv.DictReader(paragraphFile)
    responseWriter = csv.writer(responseFile)
    for paragraph in paragraphReader:
        if paragraph['number'] not in responseDictionary.keys():
            print(paragraph['text'])
            userResponse = input("\n******\nIs this related to topic? (Y/N), or quit   ")
            while userResponse not in ('y', 'n', 'Y', 'N', 'quit'):
                userResponse = input("Bad input... Is this related to topic? (Y/N)   ")
            if userResponse == 'quit':
                break
            print("your response is ", userResponse.upper(), "\n******\n")
            responseDictionary[paragraph['number']] = userResponse
            field = [paragraph['number'], userResponse.upper()]
            responseWriter.writerow(field)
responseFile.close()
