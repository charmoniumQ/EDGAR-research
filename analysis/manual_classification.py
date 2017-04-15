try:
    import traceback
    import csv
    import getch
    import os
    import textwrap
    import glob
    
    if 'analysis' in os.getcwd():
        os.chdir('..')
    print(os.getcwd())

    # Get all paragraph files in the data folder and display them to the user. Terminate if there are no paragraph files

    files = glob.glob("data/paragraphs_*_*.csv")
    if len(files) == 0:
        print("no paragraph files available :(")
        os._exit(0)

    for filePath in files:
        print("[%d] %s" % (files.index(filePath), filePath))

    # Let user select which paragraph file to work on

    userResponse = input("-Please select the paragraph file to process (enter the number displayed to the left)")
    while len(files) <= int(userResponse):
        userResponse = input("-Bad input\n-Please select the paragraph file to process "
                             "(enter the number displayed to the left)")
    
    # Login with username
    username = input("-Please enter username: ")
    while username == "":
        username = input("Error: username can't be blank\nPlease enter username: ")
    
    # File paths used
    paragraphFilePath = files[int(userResponse)]
    responseFilePath = paragraphFilePath.replace("paragraphs", "responses").replace(".csv", "") + "_" + username + ".csv"
    
    # Dictionary to hold previously entered responses
    responseDictionary = {}
    
    # create the response file if it doesn't exist; if it does exist, load it into responseDictionary
    if os.path.isfile(responseFilePath):
        with open(responseFilePath, "r", encoding='utf-8') as responseFile:
            responseReader = csv.DictReader(responseFile)
            for row in responseReader:
                responseDictionary[row['number']] = row['response']
    else:
        with(open(responseFilePath, "w+", encoding='utf-8')) as responseFile:
            responseFile.write("number,response\n")
        responseFile.close()
    
    print("+Found", len(responseDictionary), " previously recorded responses by", username, "\n")
    quit = False

    # Go through the remaining paragraphs and record the user's response in the corresponding response file.
    with open(paragraphFilePath, encoding='utf-8') as paragraphFile, open(responseFilePath, "a", newline="", encoding='utf-8') as responseFile:
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

