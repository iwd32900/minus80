#!/bin/bash
while true; do
    # Find .jpg files that haven't changed in at least 14 days:
    find ~/Pictures/iPhoto\ Library/Originals/2001 -iname '*.jpg' -mtime +14 | ./minus80.py config.json
    sleep 3600
done
