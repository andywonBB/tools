#!/bin/sh

# yesterday's date
dt=$(date --date yesterday "+%Y-%m-%d")

# file count of yesterday's weborders bucket. We should have 48 every day
x=$(hdfs dfs -count /event/weborders/$dt)
num_files=$(echo $x | grep -o " [0-9]. ")

# Message to send if a number != 48 is found
error="Oh noes! Something might be wrong with the event data. There should be 48 files in the /event/weborders directory, but I only see $num_files"


if echo $x | grep --quiet "^[1-9] 48 " ; then
	:
else
	echo $error | mail -s "Possible Event Data Issue for $dt" dsai@birchbox.com
fi
