import csv
import os.path
import getch

import os
if 'analysis' in os.getcwd():
    os.chdir('..')
print(os.getcwd())

# Login with username
username = input("-Please enter username: ")
while username == "":
    username = input("Error: username can't be blank\nPlease enter username: ")

# File paths used
paragraphFilePath = "data/paragraphs.csv"
responseFilePath = "data/responses_" + username + ".csv"

# Dictionary to hold previously entered responses
responseDictionary = {}

# create the response file if it doesn't exist; if it does exist, load it into responseDictionary
if os.path.isfile(responseFilePath):
    with open(responseFilePath, "r") as responseFile:
        responseReader = csv.DictReader(responseFile)
        for row in responseReader:
            responseDictionary[row['number']] = row['response']
else:
    with(open(responseFilePath, "w+")) as responseFile:
        responseFile.write("number,response\n")
    responseFile.close()

print("+Found", len(responseDictionary), " previously recorded responses by", username, "\n")

# Go through the remaining paragraphs and record the user's response in the corresponding response file.
with open(paragraphFilePath) as paragraphFile, open(responseFilePath, "a", newline="") as responseFile:
    paragraphReader = csv.DictReader(paragraphFile)
    responseWriter = csv.writer(responseFile)
    for paragraph in paragraphReader:
        if paragraph['number'] not in responseDictionary.keys():
            print("#%s: %s" % (paragraph['number'], paragraph['text']))
            print("-Is this related to topic? (Y/N/F), or quit   ")
            userResponse = getch.getch()
            while userResponse not in ('y', 'n', 'Y', 'N', 'f', 'F', 'quit'):
                print("-Bad input... Is this related to topic? (Y/N/F)   ")
                userResponse = getch.getch()
            if userResponse == 'quit':
                break
            print("+Your response, %s, is recorded\n" % userResponse.upper())
            responseDictionary[paragraph['number']] = userResponse
            field = [paragraph['number'], userResponse.upper()]
            responseWriter.writerow(field)
responseFile.close()
