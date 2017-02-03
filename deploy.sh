#!/bin/sh

topic="write risk factor searching"

user="sam"
host="104.196.27.128"
dest="~/EDGAR-research"
identity="~/.ssh/google_compute_engine"

git commit -m "$topic"

git push --all origin

ssh -i $identity $user@$host "cd $dest ; git pull ; cd mining ; ./extract_risk.py"
