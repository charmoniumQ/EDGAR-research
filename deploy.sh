#!/bin/sh

topic="write risk factor searching"
main="cd mining; ./extract_risk.py"
branch="testing"

user="sam"
host="104.196.27.128"
dest="~/EDGAR-research"
identity="~/.ssh/google_compute_engine"

git commit --amend -m "$topic"

git push origin $branch --force

ssh -i $identity $user@$host "cd $dest ; git pull origin $branch --force ; $main"
