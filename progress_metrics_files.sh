#! /bin/bash

# Copyright and licensing information given in the main script
# 'hindawi-downloader.py' apply.

# Quick and dirty mod of the IE count script. Now counts files.
#
# The script counts actual article files and supplementary materials if they
# exist. It ignores MD5 sidecar files or XML metadata files produced by the
# downloader.

download_folder=/enter/folder/here
ingested_folder=/enter/folder/there

todo=$(find $download_folder -type f -path "*MASTER*" ! -name "*.md5" | wc -l)
done=$(find $ingested_folder -type f -path "*MASTER*" ! -name "*.md5" | wc -l)
sum=$(expr $todo + $done)

printf "TOTAL (SUM), TO BE PROCESSED, PROCESSED\n"
printf "$sum, $todo, $done\n"
