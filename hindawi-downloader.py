# -*- coding: utf-8 -*-

# Developed by Peter Eisner

#   Copyright 2021 Technische Informationsbibliothek (TIB)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import os
import sys
import subprocess
import argparse
import logging
import datetime
import time
import requests
import re
import hashlib
import csv
import json
from bs4 import BeautifulSoup
from sickle import Sickle
from lxml import etree
from copy import copy
from shutil import rmtree


def report_rmtree_fail(function, path, excinfo):

    """Logs failure to remove a folder."""

    logger.error(f'Error while deleting folder {path}.')
    logger.error(f'rmtree reports: {excinfo}')


def parse_setfile(filename):

    """Read given setfile, return as list."""

    logger.debug(f'Creating list from setfile {filename}.')

    with open(filename, 'r') as file:
        sets = file.readlines()
        # get rid of newline characters
        sets = [i.rstrip() for i in sets]
    return sets


def parse_record_list(filename):

    """
    Read file with OAI PMH record ids, return as list.

    This function is part of the resume-from-crash feature. The downloader
    writes all remaining record ids of a set to a text file, which can be
    passed per the '--oaiid' argument, after a download stopped due to an
    exception.

    While this is not an issue for normal-sized sets, it can be quite annoying
    to see a 2000-records download fail somewhere in the 1700s. This is a
    workaround. The exceptions experienced occur in the requests (or urllib3)
    module. Further analyzing needed. There is propably a way to configure
    request's behavior, or it should be called with exception handling.
    """

    # TODO: See docstring.

    logger.debug(f'Creating list from OAI record file {filename}.')

    with open(filename, 'r') as file:
        oia_ids = file.readlines()
        # get rid of newline characters
        oai_ids = [i.rstrip() for i in oia_ids]
    return oai_ids


def map_json_to_dict(file):

    """Opens a JSON file and returns the content as a dictionary."""

    logger.debug(f'Trying to parse JSON file {file}.')

    with open(file, 'r') as jsonfile:
        jdict = json.load(jsonfile)

    return jdict


def analyze_set(current_set):

    """Decides if string is a set, subset or garbage by judging its looks."""

    logger.debug(f'Analyzing set {current_set}.')

    if current_set.split('.')[0] != 'HINDAWI':
        return 'garbage'
    elif ':' in current_set:
        return 'subset'
    else:
        return 'set'


def get_subsets(current_set):

    """Returns a list of subsets (volumes) of a given set (journal)."""

    logger.debug(f'Trying to get subsets for {current_set}.')

    setlist = sickle.ListSets()

    current_subsets = []
    for item in setlist:
        setspec = item.setSpec
        if ':' in setspec and setspec.split(':')[0] == current_set:
            current_subsets.append(setspec)
            logger.info(f'Found matching subset ({setspec}); appending list.')

    return current_subsets


def get_journal_title(current_set):

    """Returns a human readable journal title of a given set."""

    logger.debug(f'Trying to get a journal title for {current_set}.')

    setlist = sickle.ListSets()

    journal_title = None
    for item in setlist:
        setspec = item.setSpec
        if current_set in setspec and ':' not in setspec:
            journal_title = item.setName

    if journal_title:
        logger.info(f'Adding journal title "{journal_title}" for statistics.')
        return journal_title
    else:
        logger.error(f'Could not get a journal title for set {current_set}. This may lead to a crash later.')


def append_setfile(current_subsets):

    """Writes a given list into a file, one line per item."""

    if not current_subsets:
        logger.error('Given set did not return subsets. You might want to investigate.')
        return
    else:
        logger.info(f'Writing/appending setfile {cl_args.makesetfile}.')
        with open(cl_args.makesetfile, 'a') as setfile:
            for item in current_subsets:
                setfile.write(item + '\n')
        return


def get_identifier_list(current_set):

    """Retrieves record IDs of OAI-PMH set, returns them as a list."""

    # TODO: implement from/until in function get_identifier_list()

    logger.debug(f'Trying to obtain record identifiers for set {current_set}.')

    oai_record_headers = sickle.ListIdentifiers(metadataPrefix='oai_dc', set=current_set)
    identifiers = []

    for header in oai_record_headers:
        identifiers.append(header.identifier)
        logger.debug(f'Appended {header.identifier} to list of record IDs.')

    logger.info(f'Successfully obtained {str(len(identifiers))} record IDs.')
    return identifiers


# TODO: add function that reads formerly processed record headers (id, datestamp) from
# database or xml file and compares those to the current record id list. Three cases:
# a) record id differs: proceed
# b) record id matches, datestamp matches: eliminate from list, generate warning (likely wrong input)
# c) record id matches, datestamp differs: download, make new version in archive
# Case c does not have an established workflow yet.


def get_download_path(cfg_file):

    """Read download path from file."""

    with open(cfg_file, 'r') as file:
        path = file.readline().rstrip()

    logger.info(f'Trying download folder {path}.')
    return path


def create_download_folder(path):

    """Creates the folder downloads go in."""

    path_head = os.path.split(path)[0]
    if path_head == '':
        # this allows a relative path one level beneath the working dir
        pass
    elif not os.path.isdir(path_head):
        logger.error('Download folder does not exist. Please create manually or')
        logger.error(f'change to a valid destination in {config_file}.')
        logger.info('Exiting now.')
        sys.exit(1)

    if os.path.isdir(path):
        logger.info('Download folder already exists.')
        return
    else:
        os.mkdir(path)
        logger.info('Creating general download folder.')


def create_set_folder(current_set):

    """Creates a timestamped folder for the current set."""

    global set_folder_name
    set_folder_name = current_set.replace('.', '_').replace(':', '_') + '_' + timestamp
    os.mkdir(os.path.join(download_destination, set_folder_name))
    logger.info(f'Created folder {set_folder_name}.')


def create_article_folder(current_record_id):

    """Creates a subfolder for a given record id."""

    global article_folder_name
    global output_path
    global output_path_downloads
    article_folder_name = current_record_id.split(':')[2].replace('/', '_').replace('.', '_')
    output_path = os.path.join(download_destination, set_folder_name, article_folder_name)
    output_path_downloads = os.path.join(output_path, 'MASTER')
    os.makedirs(output_path_downloads)
    logger.info(f'Created subfolder {article_folder_name}.')


def save_oai_record(record):

    """Writes OAI record to file."""

    with open(os.path.join(output_path, 'oai-record.xml'), 'w') as xml_record:
        xml_record.write(record.raw)
        logger.info('Writing OAI PMH record.')


def abort():

    """Delete the remains of a failed article retrieval."""

    logger.debug(f'Attempting to remove folder {output_path}.')
    rmtree(output_path, onerror=report_rmtree_fail)


def retry_later(current_record_id):

    """Adds the current record id to the end of the list, so it gets processed again."""

    time.sleep(2*current_record_ids.count(current_record_id))

    if current_record_ids.count(current_record_id) > 3:
        if oai_set not in failed_record_ids:
            failed_record_ids[oai_set] = [current_record_id]
        else:
            failed_record_ids[oai_set].append(current_record_id)
        logger.error('Multiple attempts to retrieve this article have failed. Giving up. ---')
    else:
        current_record_ids.append(current_record_id)
        logger.info('Will retry to retrieve article later. Skipping for now. ---')


def scrape_dc_metadata(page_content):

    """Appends Dublin Core metadata from web page to a dictionary."""

    if article_url not in article_page_dc:
        article_page_dc[article_url] = {}

    dc_elements = page_content.find_all('meta', {'name': re.compile(r'dc\..*')})

    # mapping to subdictionary and list since DC elements are repeatable
    for element in dc_elements:
        dc_tag = element.get('name')
        dc_content = element.get('content')
        if dc_tag not in article_page_dc[article_url]:
            article_page_dc[article_url][dc_tag] = [dc_content]
        else:
            article_page_dc[article_url][dc_tag].append(dc_content)


def get_license_information(page_content):

    """Reads link to CC License from article page."""

    license_element = page_content.find(['a', 'ext-link'], string=re.compile(r'.*Creative\sCommons\sAttribution\sLicense.*'))
    if license_element is None:
        logger.error('Could not scrape license information from website.')
        license = 'HinJoDL: Missing license information.'
        return license

    license = license_element.get('href')
    if license is None:
        license = license_element.get('xlink:href')

    if license:
        logger.debug(f'Found license string in {article_page.url}.')
        return license
    else:
        logger.error(f'Could not extract license attribute from element in {article_page.url}.')
        license = 'HinJoDL: Missing license information.'
        return license


def get_issn(page_content):

    """Reads ISSN from article page."""

    issn_element = page_content.find('meta', {'name': 'citation_issn'})  # name is a reserved kw in bs
    issn = issn_element.get('content')

    if issn:
        logger.debug(f'Found ISSN in {article_page.url}.')
        return issn
    else:
        logger.error(f'Could not find ISSN in {article_page.url}.')

def make_xml_output(folder, current_record):

    """(Destructively) Translates oai record and other sources to custom xml records."""

    # get namespace map from oai record
    oai_nsmap = current_record.xml.find('.//{*}dc').nsmap
    kicked_default_namespace = oai_nsmap.pop(None, None)    # removes default ns

    # get elements from oai record
    dc_elements = current_record.xml.findall('.//dc:*', namespaces=oai_nsmap)
    oai_datestamp_element = current_record.xml.find('.//{*}datestamp')

    # add dcterms namespace for output
    dc_xml_nsmap = oai_nsmap
    dc_xml_nsmap['dcterms'] = 'http://purl.org/dc/terms/'

    # root element for dc.xml
    dc_xml_root = etree.Element('record', nsmap=dc_xml_nsmap)

    # remap dc elements (removing them from their origin at the same time)
    for element in dc_elements:
        dc_xml_root.append(element)

    # cut URL part from doi
    dc_identifier = dc_xml_root.find('.//dc:identifier', namespaces=dc_xml_nsmap)
    doi_with_url = dc_identifier.text
    doi_prefix = 'DOI: '
    doi_without_url = doi_prefix + doi_with_url.partition('doi.org/')[-1]
    dc_identifier.text = doi_without_url

    # remove dc:rights tag
    dc_rights = dc_xml_root.find('.//dc:rights', namespaces=dc_xml_nsmap)
    if dc_rights is not None:
        dc_rights.getparent().remove(dc_rights)

    # construct additional dcterms elements
    dc_publisher = dc_xml_root.find('.//dc:publisher', namespaces=dc_xml_nsmap)
    dc_date = dc_xml_root.find('.//dc:date', namespaces=dc_xml_nsmap)

    dc_xml_ispartof = etree.Element('{http://purl.org/dc/terms/}isPartOf')
    dc_xml_ispartof.text = f'{dc_publisher.text}/{dc_date.text}'
    dc_xml_root.append(dc_xml_ispartof)
    dc_xml_accessrights = etree.SubElement(dc_xml_root, '{http://purl.org/dc/terms/}accessRights')
    dc_xml_accessrights.text = license_string
    dc_xml_issued = etree.SubElement(dc_xml_root, '{http://purl.org/dc/terms/}issued')
    dc_xml_issued.text = oai_datestamp_element.text

    qname = etree.QName("http://www.w3.org/2001/XMLSchema-instance", "type")
    dc_xml_issn = etree.Element('{http://purl.org/dc/elements/1.1/}identifier', {qname: 'dcterms:ISSN'})
    dc_xml_issn.text = issn_string
    dc_xml_root.append(dc_xml_issn)

    # temporary hack to track dc:publisher field (also used in collections.xml, so keep it)
    global dc_publisher_string
    dc_publisher_string = dc_publisher.text

    # make elements for collections.xml
    collection_nsmap = {'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
                        'dc': 'http://purl.org/dc/elements/1.1/',
                        'dcterms': 'http://purl.org/dc/terms/'}
    collection_xml_root = etree.Element('collections', nsmap=collection_nsmap)
    collection_xml_collection = etree.SubElement(collection_xml_root, 'collection')
    collection_xml_ispartof = etree.Element('{http://purl.org/dc/terms/}isPartOf')
    collection_xml_ispartof.text = f'Open Access E-Journals/Hindawi/{dc_publisher_string}'
    collection_xml_title = etree.Element('{http://purl.org/dc/elements/1.1/}title')
    collection_xml_title.text = dc_date.text
    collection_xml_collection.append(collection_xml_ispartof)
    collection_xml_collection.append(collection_xml_title)
    collection_xml_collection.append(copy(dc_xml_issn))

    # make elements for harvest.xml
    harvest_xml_root = etree.Element('harvest')
    harvest_xml_primaryseedurl = etree.SubElement(harvest_xml_root, 'primarySeedURL')
    harvest_xml_primaryseedurl.text = base_url
    harvest_xml_wctidentifier = etree.SubElement(harvest_xml_root, 'WCTIdentifier')
    harvest_xml_wctidentifier.text = f'TIB-LZA Journal Downloader Hindawi/ Version: {hinjodl_version}'
    harvest_xml_targetname = etree.SubElement(harvest_xml_root, 'targetName')
    harvest_xml_targetname.text = oai_set
    harvest_xml_objectidentifier = etree.SubElement(harvest_xml_root, 'objectIdentifier')
    harvest_xml_objectidentifier.text = record_id
    harvest_xml_group = etree.SubElement(harvest_xml_root, 'group')
    harvest_xml_group.text = 'Hindawi Publishing Corporation'
    harvest_timestamp = datetime.datetime.today()
    harvest_xml_harvestdate = etree.SubElement(harvest_xml_root, 'harvestDate')
    harvest_xml_harvestdate.text = harvest_timestamp.strftime('%Y-%m-%d %H:%M:%S')

    # any mandatory tags missing?
    mandatory_tags = ['dc:title',
                      'dc:creator',
                      'dc:publisher',
                      'dc:date']
    for tag in mandatory_tags:
        element = dc_xml_root.find(f'.//{tag}', namespaces=dc_xml_nsmap)
        if element is None:
            logger.warning(f'Could not find mandatory DC element {tag} in oai record.')

            if tag not in missing_md:
                missing_md[tag] = [article_url]
            else:
                missing_md[tag].append(article_url)

            tag_web_dc = tag.replace(':', '.')
            if tag_web_dc in article_page_dc[article_url]:
                logger.info(f'Dublin Core metadata on article page suggests {tag} is {article_page_dc[article_url][tag_web_dc]}.')

    # write output
    dc_xml_tree = etree.ElementTree(dc_xml_root)
    collection_xml_tree = etree.ElementTree(collection_xml_root)
    harvest_xml_tree = etree.ElementTree(harvest_xml_root)

    dc_xml_tree.write(os.path.join(folder, 'dc.xml'),
                      xml_declaration=True,
                      encoding='utf-8',
                      pretty_print=True)
    harvest_xml_tree.write(os.path.join(folder, 'harvest.xml'),
                              xml_declaration=True,
                              standalone=False,
                              encoding='utf-8',
                              pretty_print=True)
    collection_xml_tree.write(os.path.join(folder, 'collection.xml'),
                              xml_declaration=True,
                              standalone=False,
                              encoding='utf-8',
                              pretty_print=True)

    logger.info('Writing XML output.')


def get_download_links(page_content):

    """Returns a list of article file URLs."""

    links = []
    download_elements = page_content.find_all('a', href=re.compile(r'.*downloads\.hindawi\.com.*'))

    # this returns some identical links, some only differ in their prefix.
    # we are stripping the "http(s)" prefix before removing the duplicates.
    # later we pragmatically assume "https" will work in all cases.
    # in case it does not, the download function will report an error.

    for element in download_elements:
        links.append(element.get('href').split('//')[1])

    links = list(set(links))            # kills duplicates

    for item in range(len(links)):
        links[item] = f'https://{links[item]}'

    link_count = len(links)

    if link_count == 0:
        logger.error('Could not get any download links from article web site.')
    else:
        logger.info(f'Extracted {link_count} unique download links from article web site.')

    return links


def download_article_files(links):

    """Iterates over a list of URLs and saves the contents."""

    for link in links:
        article_file = requests.get(link)

        if not article_file.ok:
            current_http_error = article_file.status_code
            logger.warning(f'Failed to download {link}. HTTP status code {current_http_error}.')
            if links.count(link) > 3:
                logger.error('Download failed multiple times, giving up.')
            else:
                links.append(link)
                logger.info('Will try again.')
                time.sleep(2*links.count(link))
            continue

        filename = link.split('/')[-1]
        current_path = output_path_downloads
        file_type = 'article'

        # write appendices to subfolder
        appendix_pattern = re.compile(r'\d*\.f\d*\..*')
        if re.match(appendix_pattern, filename):
            file_type = 'supplemental'
            logger.info(f'Supplemental file detectet: {link}.')
            current_path = os.path.join(output_path_downloads, 'supplements')
            if not os.path.exists(current_path):
                os.makedirs(current_path)
            global supplementary_materials_exist
            supplementary_materials_exist = True

        # write file
        with open(os.path.join(current_path, filename), 'wb') as file:
            file.write(article_file.content)
            logger.info(f'Writing {file_type} file {filename}.')

        # write md5 hash
        md5sum = hashlib.md5(article_file.content).hexdigest()
        with open(os.path.join(current_path, filename + '.md5'), 'w') as md5file:
            md5file.write(f'{md5sum}  {filename}\n')
            logger.debug(f'Writing checksum for {filename}.')


def check_file_sizes(folder):

    """Raises alarm when file sizes are suspicious."""

    logger.debug(f'Checking file sizes in {folder}.')

    files = []
    with os.scandir(folder) as contents:
        for item in contents:
            if item.is_file():
                files.append(item)

    if supplementary_materials_exist:
        with os.scandir(os.path.join(folder, 'supplements')) as sup_contents:
            for sup_item in sup_contents:
                if sup_item.is_file():
                    files.append(sup_item)
                else:
                    logger.warning('Something in "supplements" is not a file. Please investigate.')

    for file in files:
        bytesize = file.stat().st_size
        mibsize = bytesize/(1024**2)
        if bytesize == 0:
            logger.error(f'Empty file detected: {file.name}.')
        elif bytesize < 1024 and '.md5' not in file.name:
            logger.error(f'Suspiciously small file detected: {file.name}.')
        elif mibsize > 2048:            # 2 GiB
            logger.warning(f'Suspiciously large file detected: {file.name} is {mibsize} MiB.')


def look_for_article_pdf(rec_id, folder):

    """Checks if there is an article PDF."""

    # look for an article pdf
    article_pdf_pattern = re.compile(r'\d*\.pdf', re.IGNORECASE)
    files = os.listdir(folder)
    article_pdf_found = False
    for filename in files:
        if re.match(article_pdf_pattern, filename):
            article_pdf_found = True
    if not article_pdf_found:
        logger.error(f'The article PDF file seems missing for OAI record {rec_id}.')


def write_unfinished_ids(id_list):

    "Writes yet to be processed OAI PMH record ids to file."

    if id_list:
        logger.info(f'Writing list of {str(len(id_list))} remaining record ids.')
        id_list = '\n'.join(id_list)
        with open(f'{timestamp}_remaining_OAI_record_ids.txt', 'w') as rec_file:
            rec_file.writelines(id_list)
    else:
        if os.path.isfile(f'{timestamp}_remaining_OAI_record_ids.txt'):
            logger.info('Deleting now empty list of unprocessed record ids.')
            os.remove(f'{timestamp}_remaining_OAI_record_ids.txt')
        else:
            logger.warning(f'Failed to delete {timestamp}_remaining_OAI_record_ids.txt. File does not exist.')


def write_oai_statistics(oai_stats):

    """Writes set metrics gathered via OAI PMH in a CSV file."""

    with open(f'{timestamp}_counted_records.csv', 'w') as csv_file:
        csvwriter = csv.writer(csv_file)
        for journal in sorted(oai_stats):
            csv_file.write('\n')
            csvwriter.writerow([journal_titles[journal]])
            csvwriter.writerow(oai_stats[journal])
            csvwriter.writerow(oai_stats[journal].values())

    logger.info('Wrote record counts reported by OAI PMH interface to csv file.')


def report_missing_metadata():

    """Write context info about missing metadata cases to text file."""

    logger.info('Writing report on missing metadata.')

    with open(f'{timestamp}_missing_metadata.txt', 'a') as report_file:
        report_file.write(f'=== {oai_set}\n')
        report_file.write('\n')
        for tag in missing_md:
            report_file.write(f'Das Dublin Core Element {tag} fehlt bei folgenden DOIs:\n')
            for id in missing_md[tag]:
                report_file.write('---\n')
                report_file.write('\n')
                report_file.write(f'{id}\n')
                report_file.write(f"Titel: {article_page_dc[id]['dc.title'][0]}\n")
                report_file.write('\n')


def report_failed_downloads(failed_oai_ids):

    """Export a list of record IDs whose retrieval ultimately failed."""

    logger.info('Writing report on failed download attempts.')

    with open(f'{timestamp}_failed_downloads.txt', 'a') as id_file:
        id_file.write(f'{oai_set}\n')
        for oai_id in failed_oai_ids[oai_set]:
            id_file.write(f'"{oai_id}" ')
        id_file.write('\n')
        id_file.write('\n')


def track_title_madness():

    """Appends a file with journal title strings from different sources.
     Temporary function, remove later."""

    # hint: 'sort -u title_string_tracking.txt | wc -l' is very close to
    # to the number of downloaded articles.

    with open('title_string_tracking.txt', 'a') as title_file:
        title_file.write(f'{oai_set}, {record_id}, {journal_titles[journal_set]}, {dc_publisher_string}\n')


# command line argument definitions

parser = argparse.ArgumentParser(description='Download Hindawi article files per OAI-PMH set.')

parser.add_argument('oaiset',
                    type=str,
                    metavar='SET or SETFILE',
                    help='A valid Hindawi set to download or a text file containing one set per line.')
parser.add_argument('--oaiid',
                    nargs='+',
                    metavar='IDENTIFIER(S)',
                    help='Only work on this, ignore rest of set. Accepts strings or newline separated text file.')
parser.add_argument('--urllut',
                    metavar='JSONFILE',
                    help='Use URL lookup table from JSON file, containing a mapping of DOIs and corresponding Hindawi URLs.')
parser.add_argument('--countrecords',
                    action='store_true',
                    default=False,
                    help='Do not download article data, just count OAI records in (sub-)sets.')
parser.add_argument('--makesetfile',
                    metavar='SETFILE',
                    help='Creates a text file with a list of subsets of a given set.')
parser.add_argument('--loglevel',
                    default='INFO',
                    metavar='LEVEL',
                    help='Standard Python log levels. Set to DEBUG for a nice, bloated log file.')

cl_args = parser.parse_args()


# start parameters

# human readable timestamp used in output file names
now = datetime.datetime.today()
timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')

# configure logging

loglevel = getattr(logging, cl_args.loglevel.upper(), None)

logger = logging.getLogger()    # using root logger for now
logger.setLevel(loglevel)  # to log module messages as well

formatter_file = logging.Formatter('%(asctime)s   %(levelname)-8s   %(message)s   (%(name)s)')
formatter_stream = logging.Formatter('%(levelname)-8s   %(message)s')

file_handler = logging.FileHandler(f'{timestamp}_hindownload.log')
file_handler.setFormatter(formatter_file)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter_stream)
stream_handler.setLevel(logging.INFO)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# log given command line parameters
logger.debug(f'oaiset is {cl_args.oaiset}.')
logger.debug(f'countrecords is {cl_args.countrecords}.')
logger.debug(f'makesetfile is {cl_args.makesetfile}.')
logger.debug(f'loglevel is {cl_args.loglevel}.')

# version of this script is latest commit datetime
git_query = subprocess.Popen(['git', 'log', '-1', '--format=%cd'], stdout=subprocess.PIPE)
git_response = git_query.communicate()
hinjodl_version = git_response[0].decode("utf-8").rstrip()
if not hinjodl_version:
    logger.error('Can not determine my version. Is git missing?')
    logger.info('Exiting gracefully.')
    sys.exit(1)
logger.info(f'TIB-LZA Journal Downloader Hindawi/ Version: {hinjodl_version}')

# get oai set list (or single set) from command line
if os.path.isfile(cl_args.oaiset):
    oai_set_list = parse_setfile(cl_args.oaiset)
else:
    oai_set_list = [cl_args.oaiset]
logger.debug(f'OAI set list is now {oai_set_list}.')

oai_set_list_original_length = len(oai_set_list)        # used for program logic

# determine program logic from command line options

# defaults
only_make_setfile = False
enable_download = True
countrecords = False
makesetfile = False
target_custom_records = False
custom_url_mapping = False

# map cl_args falsiness for readability
if cl_args.countrecords:
    countrecords = True
if cl_args.makesetfile:
    makesetfile = True
if cl_args.oaiid:
    target_custom_records = True
    if os.path.isfile(cl_args.oaiid[0]):
        custom_records = parse_record_list(cl_args.oaiid[0])
    else:
        custom_records = cl_args.oaiid
if cl_args.urllut:
    custom_url_mapping = True

# is makesetfile used without countrecords?
if makesetfile and not countrecords:
    only_make_setfile = True

# disable download when not desired
if cl_args.countrecords or cl_args.makesetfile:
    enable_download = False
    logger.info('Article download in this run disabled.')

# read the config file
config_file = 'download_to.cfg'
download_destination = get_download_path(config_file)

# initialize dictionaries or lists for statistics and structural information
journal_titles = {}            # dictionary: {'setSpec': 'Journal Title'}
set_statistics = {}            # used as nested dictionary:
                               # {'set': {'set': n, 'subset': m, ...}}
record_ids_of_set = {}         # preserves correlation between set and oai identifiers
article_page_dc = {}           # Dublin Core metadata scraped from article web page
failed_record_ids = {}         # download for these oai records failed

# create url lookup table if given
if custom_url_mapping is True:
    doi_url_map = map_json_to_dict(cl_args.urllut)

# initialize oai pmh harvester
base_url = 'https://www.hindawi.com/oai-pmh/oai.aspx'
sickle = Sickle(base_url)
logger.info('OAI-PMH harvester initialized.')


# main program

create_download_folder(download_destination)

for set_index, oai_set in enumerate(oai_set_list, start=1):

    logger.info(f'=== Working on set {oai_set[:32]}.')  # "set" >32 is definitely wrong input
    set_type = analyze_set(oai_set)
    if set_type == 'garbage':
        logger.warning('Sorry, this does not look like a valid Hindawi set. Skipping.')
        continue

    if makesetfile and set_index <= oai_set_list_original_length:
        if set_type == 'subset':
            logger.warning('Sorry, can not make a setfile from a subset. Skipping.')
            continue
        else:
            subsets = get_subsets(oai_set)
            append_setfile(subsets)

    if not only_make_setfile:

        # append corresponding subsets in countrecords mode
        if countrecords and set_type == 'set':
            subsets_to_count = get_subsets(oai_set)
            oai_set_list.extend(subsets_to_count)

        journal_set = oai_set.split(':')[0]

        # get journal title if necessary
        if journal_set not in journal_titles:
            journal_titles[journal_set] = get_journal_title(journal_set)

        # retrieve record identifiers
        if not target_custom_records:
            record_ids_of_set[oai_set] = get_identifier_list(oai_set)
            current_record_ids = record_ids_of_set[oai_set]
        else:
            record_ids_of_set[oai_set] = custom_records
            current_record_ids = record_ids_of_set[oai_set]
            logger.info(f'Targeting only {len(current_record_ids)} given OAI records.')

        # count records in set or subset, put in stats
        if journal_set not in set_statistics:                  # subdictionary
            set_statistics[journal_set] = {}                   # per journal
        set_statistics[journal_set][oai_set] = len(current_record_ids)

    if enable_download is True:
        create_set_folder(oai_set)
        missing_md = {}             # stores cases of missing DC metadata
        logger.debug('Flushing missing metadata collection.')

        # resume after crash -- copy record id list for tracking actually
        # downloaded articles
        unprocessed_rec_ids = current_record_ids.copy()

        # per article loop
        for record_id in current_record_ids:

            logger.info(f'--- Working on record {record_id}.')

            # reset to default
            supplementary_materials_exist = False

            # get OAI record
            create_article_folder(record_id)
            oai_record = sickle.GetRecord(identifier=record_id, metadataprefix='oai_dc')
            save_oai_record(oai_record)

            # get url for scraping content (this is usually a doi from dc:identifier)
            article_url = oai_record.metadata.get('identifier', ['nobunny'])[0]
            if article_url == 'nobunny':
                logger.error('Could not get DOI from OAI record. Skipping. ---')
                continue
            else:
                logger.info(f'Extracted DOI from OAI record: {article_url}.')

            # remap URL when URL lookup table is provided
            if custom_url_mapping:
                if article_url in doi_url_map:
                    article_url = doi_url_map[article_url]

            # retrieve article web site (follows redirect by default)
            try:
                article_page = requests.get(article_url)
            except requests.exceptions.ChunkedEncodingError:
                logger.warning(f'Failed to get article page. Exception from requests module.')
                retry_later(record_id)
                abort()
                continue

            if not article_page.ok:
                http_error = article_page.status_code
                logger.warning(f'Could not retrieve article page. HTTP status code {http_error}.')
                retry_later(record_id)
                abort()
                continue

            if 'hindawi.com' not in article_page.url:
                logger.error(f'The DOI points to a third party source: {article_page.url}. Skipping. ---')
                continue

            article_page_content = BeautifulSoup(article_page.text, 'lxml')
            logger.info(f'Retrieved article web site {article_page.url}.')

            scrape_dc_metadata(article_page_content)

            license_string = get_license_information(article_page_content)
            issn_string = get_issn(article_page_content)
            make_xml_output(output_path, oai_record)
            track_title_madness()          # temporary hack (remove function, clean make_xml_output)

            download_links = get_download_links(article_page_content)
            download_article_files(download_links)

            check_file_sizes(output_path_downloads)
            look_for_article_pdf(record_id, output_path_downloads)

            unprocessed_rec_ids.remove(record_id)
            write_unfinished_ids(unprocessed_rec_ids)

            logger.info(f'Processed article. ---')

        if missing_md:
            report_missing_metadata()

        if oai_set in failed_record_ids:
            report_failed_downloads(failed_record_ids)


# export statistics
if not only_make_setfile:
    write_oai_statistics(set_statistics)

# inform user when errors or warnings occurred
if 30 in logger._cache and logger._cache[30]:
    logger.info('-  THERE HAVE BEEN WARNINGS.  - Please check the logfile.')
if 40 in logger._cache and logger._cache[40]:
    logger.info('-  THERE HAVE BEEN ERRORS.  - Please check the logfile.')

logger.info('Parsed all given sets.\nDone.')
