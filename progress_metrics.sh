#! /bin/bash

# Copyright and licensing information given in the main script
# 'hindawi-downloader.py' apply.

# This script shows how much of the targeted Hindawi articles are alreaady
# downloaded or processed. An article represents one IE in the archive.
# The output is all downloaded IEs, yet to be ingested, already ingestet, and
# finally the remaining IEs.
# Should work fine as long as the folder structure is intact and processed
# files are not deleted.

download_folder=/enter/folder/here
ingested_folder=/enter/folder/there
total_ies=41168

todo=$(find $download_folder -type d -name "MASTER" | wc -l)
done=$(find $ingested_folder -type d -name "MASTER" | wc -l)
sum=$(expr $todo + $done)
remaining=$(expr $total_ies - $sum)

printf "DOWNLOADED (SUM), TO BE PROCESSED, PROCESSED, REMAINING\n"
printf "$sum, $todo, $done, $remaining\n"
