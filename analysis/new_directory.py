import os
import os.path

def new_directory(make=True):
    directory = os.path.join('results', 'result_')
    i = 0
    while os.path.isdir(directory + str(i)) or os.path.isfile(directory + str(i)):
        i += 1
    directory += str(i)
    if make:
        os.mkdir(directory)
    return directory + os.path.sep
