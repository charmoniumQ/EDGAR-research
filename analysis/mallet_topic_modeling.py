import os
from util.new_directory import new_directory

'''
This script automatically runs MALLET's topic modeling algorithm on data provided by section_extract.py
-MALLET_PATH is the absolute path to the directory where mallet files are stored. 
        Mallet can be downloaded from http://mallet.cs.umass.edu/download.php
-INPUT is the folder where the results of section_extract.py are saved 
        (a folder containing text files and nothing else)
-NUMBER_OF_TOPICS is a number set by us; tells the algorithm how many topics we want to get from our data
'''

MALLET_PATH = '/home/shayan/Programs/mallet-2.0.8'
INPUT = '/home/shayan/Documents/UTD/EDGAR/EDGAR-research/results/result_99'
NUMBER_OF_TOPICS = 80
OUTPUT_PATH = os.getcwd()/new_directory()

print('Output path = ' + OUTPUT_PATH.as_posix())
print('*changing dir to mallet path', MALLET_PATH)
os.chdir(MALLET_PATH)
print('*importing data')
os.system('bin/mallet import-dir --input ' + INPUT + ' --output ' + OUTPUT_PATH.as_posix() + '/topic-input.mallet \
  --keep-sequence --remove-stopwords')
print('*topic modeling')
os.system('bin/mallet train-topics --input ' + OUTPUT_PATH.as_posix() + '/topic-input.mallet \
  --num-topics ' + str(NUMBER_OF_TOPICS) + ' --output-state ' + OUTPUT_PATH.as_posix() + '/topic-state.gz')
