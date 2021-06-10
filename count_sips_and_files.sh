#! /bin/bash

# Copyright and licensing information given in the main script
# 'hindawi-downloader.py' apply.

# helper script that does some statistics on Hindawi Journal Downloader
# output, ie downloaded content. Needs to run where the set folders are.
# Redirect the output to a CSV file, or pipe it to "column -s ',' -t".


printf "SET, DC:DATE, SIZE, SIPs, SIP FILES, DIFF MD5s, DCTERMS:ISSN, DC:PUBLISHER\n"

for set_folder in *HINDAWI*/
do
  hindawi_set=$(cut -d '_' -f2-3 <<< $set_folder)
  dc_date=$(find $set_folder -name "dc.xml" -exec grep "dc:date" {} \; | sort -u | sed -e 's/ *<[^>]*>//g' | tr '\n' ' ')
  dc_publisher=$(find $set_folder -name "dc.xml" -exec grep "dc:publisher" {} \; | sort -u | sed -e 's/ *<[^>]*>//g' | tr '\n' ' ')
  dc_issn=$(find $set_folder -name "dc.xml" -exec grep "dcterms:ISSN" {} \; | sort -u | sed -e 's/ *<[^>]*>//g' | tr '\n' ' ')
  size=$(du -s -BM  $set_folder | cut -f1)
  sip_folders=$(find $set_folder -mindepth 1 -maxdepth 1 -type d | wc -l)
  sip_files=$(find $set_folder -type f -path "*MASTER*" ! -name "*.md5"| wc -l)
  md5_files=$(find $set_folder -type f -path "*MASTER*" -name "*.md5"| wc -l)
  diff_sip_md5=$(expr $sip_files - $md5_files)

  printf "$hindawi_set, $dc_date, $size, $sip_folders, $sip_files, $diff_sip_md5, $dc_issn, $dc_publisher\n"
done
