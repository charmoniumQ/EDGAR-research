#!/bin/bash

# run on remote machine in the working directory of the project

cd `dirname $0`
cd ..

. cluster_config/info.sh

topic="write risk factor searching"

git commit -a --amend -m "$topic" || exit 1

git push origin $branch --force || exit 1

if [ "$1" = "install" ]
then
	ssh -i $identity $user@$host "rm -rf $dest ; git clone $clone_url -b $branch $dest ; cd $dest ; ./cluster_config/remote_install.sh"
else
	ssh -i $identity $user@$host "rm -rf $dest ; git clone $clone_url -b $branch $dest ; cd $dest ; ./cluster_config/remote_install.sh"
	#ssh -i $identity $user@$host "cd $dest ; git pull origin $branch ; ./cluster_config/remote_install.sh"
fi
