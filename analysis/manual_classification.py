try:
    import traceback
    import csv
    import getch
    import os
    import textwrap
    
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
    quit = False


    # Go through the remaining paragraphs and record the user's response in the corresponding response file.
    with open(paragraphFilePath) as paragraphFile, open(responseFilePath, "a", newline="") as responseFile:
        paragraphReader = csv.DictReader(paragraphFile)
        responseWriter = csv.writer(responseFile)
        for paragraph in paragraphReader:
            if paragraph['number'] not in responseDictionary.keys():
                print("#%s [%s]:\n %s" % (paragraph['number'], paragraph['address'], textwrap.fill(paragraph['text'])))
                print("-Is this related to topic? (Y/N/F), or q to quit   ")
                userResponse = getch.getch()
                if isinstance(userResponse, bytes):
                    userResponse = userResponse.decode()
                while userResponse not in ('y', 'n', 'Y', 'N', 'f', 'F', 'q', 'Q'):
                    print("-Bad input... Is this related to topic? (Y/N/F), q to quit   ")
                    userResponse = getch.getch()
                if userResponse.lower() == 'q':
                    quit = True
                    break
                print("+Your response, %s, is recorded\n" % userResponse.upper())
                responseDictionary[paragraph['number']] = userResponse
                field = [paragraph['number'], userResponse.upper()]
                responseWriter.writerow(field)
    responseFile.close()
    if not quit:
        getch.pause("DONE! Press any key to continue...")

except:
    traceback.print_exc()

