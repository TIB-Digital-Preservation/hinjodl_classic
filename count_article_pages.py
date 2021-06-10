# -*- coding: utf-8 -*-

# Copyright and licensing information given in the main script
# 'hindawi-downloader.py' apply.

# This helper script counts unique links to article pages from a Hindawi
# journal's volume navigation.
#
# These article counts can be used for plausibility checks in regard to
# completeness of a given volume. Sourcing the numbers from the website
# differs from the downloader's approach to gather data via OAI PMH. A
# mismatch between both sources means trouble.
#
# As is, "url_title_component" and  "url_harvesting_range" are considered
# user input.
#
# To generate a sequence of volumes for now:
# 1. Check on website which one is the first volume (eg. 1978)
# 2. Do something like this (in a shell)
#    seq -s ', ' -f "'%04g'" 1978 2019
# 3. Copy the sequence in the "url_harvesting_range" list.
#
# Output will be a CSV file named <title_handle>_website_article_count.csv.
#
# Some OAI PMH sets do not follow the year = volume logic, so there might
# be some manual steps left to do.


import datetime
import time
import requests
import re
from bs4 import BeautifulSoup


# scraping target  (title component and harvesting range = user input)
hindawi = 'https://www.hindawi.com'
url_base = 'https://www.hindawi.com/journals'
url_title_component = 'jpol'
url_volume_component = 'contents/year'
url_harvesting_range = ['2016']
url_page_component = 'page'

# human readable timestamp used in output file name
now = datetime.datetime.today()
timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')

# initialize some globals
volume_stats = {}


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


# main program

# scrape article urls

for volume in url_harvesting_range:

    last_page_reached = False
    url_page_number = 1
    article_urls = []

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
            time.sleep(121)
            url_page_number += 1
            continue
            
        print(f'Working on {navi_page.url}.')

        navi_page_content = BeautifulSoup(navi_page.text, 'lxml')

        current_article_urls = scrape_article_urls(navi_page_content)

        if len(current_article_urls) != 0:
            url_page_number += 1
            article_urls.extend(current_article_urls)
        else:
            last_page_reached = True

        time.sleep(5)                   # prevent 403, hopefully

    volume_stats[volume] = len(article_urls)


# export stats as CSV

with open(f'{url_title_component}_website_article_count.csv', 'w') as csv_file:
    csv_file.write('\n')
    csv_file.write(f'Articles per Volume sourced from Hindawi website. Journal: {url_title_component}. Checked on {timestamp}.\n')
    csv_file.write('\n')
    csv_file.write('YEAR, ARTICLES\n')
    for volume in volume_stats:
        print(volume, volume_stats[volume], sep=',', end='\n', file=csv_file)
