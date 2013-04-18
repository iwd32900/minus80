#!/bin/bash
while true; do
    # Find .jpg files that haven't changed in at least 14 days:
    find ~/Pictures/iPhoto\ Library/Originals/2001 -iname '*.jpg' -mtime +14 | ./minus80.py archive config.json
    # Wait 1 hour, then run again:
    sleep 3600
done