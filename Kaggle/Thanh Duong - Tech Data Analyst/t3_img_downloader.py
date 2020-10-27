#!/usr/bin/env python3
# File name: t3_img_downloader.py
# Description: The Python scripts for select and download an image from a web
# Author: Thanh Duong
# Date: 28-07-2020

import datetime
import os
import re
import requests

from io import BytesIO
from urllib.parse import urljoin, urlparse

from PIL import Image
from bs4 import BeautifulSoup


def is_img(url_text):
    if not url_text:
        return False
    re_patten = r'[-\w]+\.(?:jpg|gif|png|jpeg)$'
    is_true = re.search(re_patten, url_text.lower())
    if is_true:
        return True
    else:
        return False


def check_urls(urls):
    imgs = []
    for u in urls:
        if is_img(u):
            imgs.append(u)
    return imgs


def is_url(url_text):
    if not url_text:
        return False
    url_pattern = r'^(?:http|ftp)s?://(?:www\.|)'
    is_true = re.search(url_pattern, url_text.lower())
    if is_true:
        return True
    else:
        return False


def reformat_url(urls, page_url):
    if not urls:
        return urls
    urls = list(urls)
    for idx, u in enumerate(urls):
        if is_url(u):
            continue
        else:
            flag = True
            while flag:
                if u.startswith('/'):
                    u = u.strip('/')
                    flag = True
                    continue
                if u.startswith('.'):
                    flag = True
                    u = u.strip('.')
                    continue
                flag = False
            urls[idx] = urljoin(page_url, u)
    return urls


def img_download(url):
    url_parsed = urlparse(url)
    img_name = os.path.basename(url_parsed.path)
    session = requests.Session()
    res = session.get(url)
    if res.status_code == 200:
        i = Image.open(BytesIO(res.content))
        print(os.path.join(os.path.dirname(__file__), img_name))
        i.save(os.path.join(os.path.dirname(__file__), img_name))
        print('Downloaded {}'.format(img_name))
    else:
        print('Please check your image url, status code --{}--'.format(res.status_code))

def main():
    try:
        page_url = input(
            "Enter your homepage with http:// format -- ex. https://baemin.com : ")
        # page_url = 'https://baemin.vn/'
        session = requests.Session()
        res = session.get(page_url)
        if res.status_code != 200:
            print('Please check your website, status code --{}--'.format(res.status_code))
        soup = BeautifulSoup(res.text, 'html.parser')
        img_tags = soup.find_all('img')
        img_urls = [img.get('src') for img in img_tags]
        data_src_urls = [img.get('data-src') for img in img_tags]
        if data_src_urls:
            img_urls.extend(data_src_urls)
            img_urls = set(img_urls)
        link_tags = soup.find_all('link')
        link_urls = set([link.get('href') for link in link_tags])
        meta_tags = soup.find_all('meta')
        meta_urls = set([meta.get('content') for meta in meta_tags])
        urls = img_urls.union(link_urls)
        urls = urls.union(meta_urls)
        img_urls_checked = check_urls(urls)
        urls_reformated = reformat_url(img_urls_checked, page_url)
        for idx, url in enumerate(urls_reformated):
            print('{}     {} \n'.format(idx, url))
        val = input("Enter the index image to download: ")
        img_download(urls_reformated[int(val)])
    except:
        print('Please rerun other website!')

if __name__ == "__main__":
    main()
