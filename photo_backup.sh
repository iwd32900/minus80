#!/bin/bash
while true; do
    # Find .jpg files that haven't changed in at least 14 days:
    find ~/Pictures/*.photoslibrary/{Masters,Previews} -iname '*.jpg' -mtime +14 | /Users/ian/anaconda/bin/python ./minus80.py archive config.json
    # For testing purposes:
    # find ~/Pictures/iPhoto\ Library/Originals/2001 -iname '*.jpg' -mtime +14 | sort | head -10 | ./minus80.py archive config.json
    # Wait 1 hour, then run again:
    sleep 3600
done
