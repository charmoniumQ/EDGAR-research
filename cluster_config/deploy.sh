#!/bin/bash

# run on remote machine in the working directory of the project

cd `dirname $0`
cd ..

. cluster_config/info.sh

rsync --archive --verbose --human-readable --compress --rsh="ssh -i $identity" --cvs-exclude . $user@$host:$dest \
	  --exclude="*/__pycache__/*" --exclude="results/*" --exclude="mining/edgar-downloads/*"

if [[ "$setup" = "yes" ]]
then
   ssh -i $identity $user@$host <<EOF
sudo apt-get install -y python
EOF
fi	

if [[ "$reinstall" = "yes" ]]
then
   ssh -i $identity $user@$host <<EOF
cd $dest
rm dist/*.egg
python2 setup.py bdist_egg
sudo easy_install-2.7 dist/*.egg
EOF
fi

ssh -i $identity $user@$host <<EOF
export PYSPARK_PYTHON=python2
cd $dest
$main
EOF
