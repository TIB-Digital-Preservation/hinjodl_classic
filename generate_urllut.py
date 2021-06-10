# -*- coding: utf-8 -*-

# Copyright and licensing information given in the main script
# 'hindawi-downloader.py' apply.

# The journal downloader fails in some cases, when DOIs provided in the OAI PMH
# metadata records point to a third party source, allthough there is a perfectly
# fine article website on Hindawi. Technically this is not an error. It affects
# older articles that originally were not published by Hindawi.
# The downloader depends on the DOI to retrieve an article website, but can not
# parse third party article websites (it is not generic, plus, there might be a
# paywall).
# Since the corresponding Hindawi article websites exist, there is a workaround.
# Using the '--urllut' option provides the missing info to the downloader via a
# JSON file. This is simple lookup table (DOI ---> Hindawi article URL).
# For single or few affected articles this JSON file can be written by hand.
#
# This is a quick and dirty script generating such a JSON file. It can be targeted
# on several volumes of a given journal. The data is gathered by scraping the
# navigation and article websites from Hindawi.


import datetime
import requests
import re
import json
from bs4 import BeautifulSoup


# scraping target  (title component and harvesting range = user input)
hindawi = 'https://www.hindawi.com'
url_base = 'https://www.hindawi.com/journals'
url_title_component = 'misy'
url_volume_component = 'contents/year'
url_harvesting_range = ['2009', '2010', '2011', '2012', '2013', '2014']
url_page_component = 'page'

# human readable timestamp used in output file name
now = datetime.datetime.today()
timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')

# initialize some globals
article_urls = []
url_doi_map = {}


def construct_url(*components):

    """Concetenate strings to URL."""

    separator = '/'
    url = separator.join(components)
    return url


def scrape_article_urls(page_content):

    """Squeezes full article URLs from navigational page."""

    article_url_pattern = re.compile(rf'/journals/{url_title_component}/{volume}/\d+/$')
    article_url_elements = page_content.find_all('a', href=article_url_pattern)

    article_url_components = []
    for url_element in article_url_elements:
        url_component = url_element.get('href')

        article_url_components.append(url_component)

    urls = []
    for component in article_url_components:
        url = hindawi + component
        urls.append(url)

    return urls


def scrape_doi(page_content):

    """Returns DOI from article page."""

    doi_element = page_content.find('meta', {'name': 'dc.identifier'})
    doi = doi_element.get('content', 'nobunny')

    if doi != 'nobunny':
        return doi
    else:
        print(f'WARNING: Could not get DOI element from {article_page.url}.')


# main program

# scrape article urls

for volume in url_harvesting_range:

    last_page_reached = False
    url_page_number = 1

    while not last_page_reached:
        target_url = construct_url(url_base,
                                   url_title_component,
                                   url_volume_component,
                                   volume,
                                   url_page_component,
                                   str(url_page_number))

        navi_page = requests.get(target_url)
        if not navi_page.ok:
            print(f'WARNING: {navi_page.url} fails with HTTP error {navi_page.status_code}.')
            continue

        navi_page_content = BeautifulSoup(navi_page.text, 'lxml')

        current_article_urls = scrape_article_urls(navi_page_content)

        if len(current_article_urls) != 0:
            url_page_number += 1
            article_urls.extend(current_article_urls)
        else:
            last_page_reached = True


# scrape DOIs from article sites

for article_url in article_urls:

    article_page = requests.get(article_url)
    if not article_page.ok:
        print(f'WARNING: {article_page.url} fails with HTTP error {article_page.status_code}.')
        continue

    article_page_content = BeautifulSoup(article_page.text, 'lxml')

    article_doi = scrape_doi(article_page_content)

    if article_doi is not None:
        url_doi_map[article_doi] = article_url


# export lookup table as JSON file

with open(f'{url_title_component}_urllut_{timestamp}.json', 'w') as json_file:
    json.dump(url_doi_map, json_file, indent=4)
