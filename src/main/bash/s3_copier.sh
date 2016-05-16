#!/bin/bash

NEWPATH=.
S3_PATH=s3://cesar-usa-tweets/

if [[ $# = 1 ]]
then
	NEWPATH=$1
fi

echo "Syncing $NEWPATH"

for F in $NEWPATH/statuses.log.*.gz
do
	/usr/local/bin/aws s3 cp $F $S3_PATH

	if [ $? -eq 0 ]
	then
		rm $F
	fi

done
