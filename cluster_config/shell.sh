#!/bin/bash

. info.sh

echo "ssh -i $identity $user@$host"
ssh -i $identity $user@$host
