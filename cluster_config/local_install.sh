#!/bin/bash

# run on remote machine in the working directory of the project

cd `dirname $0`
cd ..

echo $(pwd)

. cluster_config/info.sh

topic="write risk factor searching"

git commit -a --amend -m "$topic"

git push origin $branch --force

ssh -i $identity $user@$host "rm -rf $dest ; git clone $clone_url -b $branch $dest ; cd $dest ; ./cluster_config/remote_install.sh"
