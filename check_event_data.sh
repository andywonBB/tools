#!/bin/sh

# yesterday's date
dt=$(date --date today "+%Y-%m-%d")

# file count of yesterday's weborders bucket. We should have 48 every day
x=$(hdfs dfs -ls /event/weborders/$dt)
num_files=$(echo $x | grep -o "tmp" | wc -l)
y=2

# Message to send if a number != 48 is found
error="Oh noes! This is an automated message. Something might be wrong with the event data. I don't see 2 tmp files in the weborders bucket! A Flume agent may be down"

# Email list
send_list=$(echo andy@birchbox.com klai@birchbox.com girish@birchbox.com )

if [ $num_files == $y ] ; then
	:
else
	echo $error | mail -s "Possible Event Data Issue for $dt" $send_list
fi
