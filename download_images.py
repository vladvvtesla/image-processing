

"""

1. Open transient's URL
2. Find direct urls and download transient's .jpeg file, dss-file, sdss and other files
and save to the directory on file system
3. If files have been successfully downloaded, UPDATE 'transients' table in DB

4. Requirements: requests, BeautifulSoup, psycopg2-binary

5. Using Example: 
$ python download_images.py --url 'url'
"""

import requests
import psycopg2
from requests.auth import HTTPBasicAuth
from bs4 import BeautifulSoup
from urllib.parse import urlsplit, parse_qsl
import argparse
import configparser
import logging
import os
import re
import csv
from genfunc import id_to_url


script_name = 'download_images.py'
script_version = 'v.0.1_20200701'

cfg_dir = '/home/vladvv/PycharmProjects/vimp10k/etc/'
cfg_f = 'tr_view.cfg'
log_dir = '/home/vladvv/PycharmProjects/vimp10k/log/'
log_f = 'download_images.log'
im_dir = '/trview/imdata/'

uname = 'uname'     # HTTPS username
pswd = 'pswd'     # HTTPS password


cfg_path = os.path.join(cfg_dir, cfg_f)

def get_html(url, uname, pswd):
    """
    Open URL.  If connection error, write log string and return empty string
    :param url:  Url for transients page
    :return html: HTML-code
    """
    auth = HTTPBasicAuth(uname, pswd)
    try:
        r = requests.get(url=url, auth=auth, verify=False)
        html = r.text
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        print('requests.get() ended up with error:', e)
        logging.info("requests.get() ended up with error: %s" % e)
        html = ''
    return html


def get_frame(data):
    soup = BeautifulSoup(data, 'lxml')
    frames = soup.find_all('frame')
    # Filter prev.php or traadd.php or trm.php
    im_frames = [frame for frame in frames if 'but' != frame.get('src')[:3]]
    return [frame.get('src') for frame in im_frames]

def get_img_url(data, main_page):
    soup = BeautifulSoup(data, 'lxml')
    tags = soup.find_all('img')

    m_page = main_page[:-9] 
    res = []
    im_urls = (tag.get('src') for tag in tags if 'site' != tag.get('src').split('/')[3]) # if no site in img_url
    for url in im_urls:
        if url[1:4] == 'cgi':
            res.append(m_page + url)
        else:
            res.append(url)

    return res

def get_tr_params(trid, data):
    """
    Parse html file trm.php?...  And get a dictionary of transient's metadata
    :param data:  html-code
    :return:  res_dict
    """
    res_dict = {'id': trid}  # Init result dict with first param
    keys = ['id', 'datetime', 'coord2000' , 'mag', 'Band', 'Limit', 'flux', 's/n',
            'xc','yc', 'fwhm', 'a', 'b', 'PA', 'N', 'C', 'Gal', 'd_ra', 'ddec',
            'dmag', 'Instrum', 'User']
    soup = BeautifulSoup(data, 'lxml')

    td_title = soup.find(title=re.compile("proc_id"))     # get datetime
    res_dict['datetime']=str(td_title.text)
    tds = td_title.find_next_siblings('td', limit=20)     # get other params
    for i in range(2,22):
        res_dict[keys[i]] = str(tds[i-2].text)
    res_dict.pop('Instrum')                         # remove param 'Instrum'

    return res_dict

def create_dir(wd, params_dict):
    """
    Get data from key 'datatime'. Create dir. Write dict to csv-file
    :param wd: workdir
    :param params_dict:
    :return: dir name like '/2020/06/20/30092342/
    """
    trid = params_dict['id']
    yymmdd = params_dict['datetime']
    year = yymmdd.split('-')[0]
    month = yymmdd.split('-')[1]
    day = yymmdd.split()[0].split('-')[2]
    res_file = str(trid) + '.csv'
    res_dir = os.path.join(wd, year, month, day, trid)

    try:
        os.makedirs(res_dir, mode=0o770, exist_ok=True)
    except OSError:
        print("Creation of the directory %s failed" % res_dir)

    csv_columns = params_dict.keys()
    dict_data = [params_dict]
    csv_file = os.path.join(res_dir, res_file)
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            for data in dict_data:
                writer.writerow(data)
    except IOError:
        print("Error during creating csv_file with transient_prams: I/O error")

    return  res_dir


def get_obsid(url, cfg_path):
    """
    Parse configuration file and return Observatory ID based on url
    :param cfg_path:
    :return:
    """
    config = configparser.ConfigParser()
    config.read(cfg_path)
    sections = config.sections()

    # Get 'tavrida' or 'iac', etc
    parsed_url = urlsplit(url)
    netloc = parsed_url.netloc
    obs = str(netloc).split('.')[0]

    for section in sections:
        if config[section]['dns_name'].split('.')[0] == obs:
            obs_id = config[section]['obs_id']

    return obs_id


def check_trid_indb(trid):
    """
    Select id, tr from 'transients' table And return True or False
    :param trid: transient id
    :return: (True, True) or (True, False) or (False, False)
    """

    try:
        conn = psycopg2.connect("host=localhost dbname=trview user=uname password=pswd")
        cur = conn.cursor()
        cur.execute("SELECT id, tr FROM transients WHERE id=%s;", (trid,))
        db_data = cur.fetchall()
        conn.commit()
        # print('check_db_data', db_data)
        if db_data:
            if db_data[0][0] and db_data[0][1]:
                return 1  # Transient already exists. Ignore it
            elif db_data[0][0] and not db_data[0][1]:
                return 2  # Transient's ID exists but 'tr' != True. Download files and add new params
        else:
                return 3  # Transient not exists. Download files and insert all params
    except psycopg2.Error as e:
        logging.info("check_trid_indb(trid) ended up with error N 502: %s" % e)
        return 502   # Error 502. Error during connection to DB

def insert_param_to_db(prm):
    """
    Connect to Database and insert Transient Parameters to table
    :param prm: Dictionary of transients parameters
    :return: True if Insert OK else False
    """
    try:
        conn = psycopg2.connect("host=localhost dbname=trview user=uname password=pwd")
        cur = conn.cursor()
        cur.execute("INSERT INTO transients VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                                                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                                                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (prm['id'], prm['datetime'], prm['coord2000'], prm['mag'],
                 prm['Band'], prm['Limit'], prm['flux'], prm['s/n'], prm['xc'], prm['yc'],
                 prm['fwhm'], prm['a'], prm['b'], prm['PA'], prm['N'], prm['C'],
                 prm['Gal'], prm['d_ra'], prm['ddec'], prm['dmag'], prm['User'], prm['obs_id'],
                 prm['path'], prm['tr'], prm['dss'], prm['sub'], prm['sdss'],
                 prm['second_lap'], prm['max_limit'], prm['log'], prm['early']
                 ))
        conn.commit()
        return True
    except psycopg2.Error as e:
        logging.info("insert_param_to_db(prm) ended up with error: %s" % e)
        return False


def add_param_to_db(prm):
    """
    Get some values from dict and add params to DB into 'transients' table
    :param tr_params: dict {'id': 12345345, 'coord': '', ...}
    :return:  True or False
    """
    # NSERT INTO test (num, data) VALUES (%s, %s)",
    # ...(100, "abc'def")


    try:
        conn = psycopg2.connect("host=localhost dbname=trview user=uname password=pswd")
        cur = conn.cursor()
        cur.execute("UPDATE transients SET "
                    "path=%s, tr=%s, dss=%s, sub=%s, sdss=%s, second_lap=%s,"
                    "max_limit=%s, log=%s, early=%s WHERE id = %s",
                    (prm['path'], prm['tr'], prm['dss'], prm['sub'], prm['sdss'],
                    prm['second_lap'], prm['max_limit'], prm['log'], prm['early'], prm['id']
                    ))
        conn.commit()
        logging.info("New params successfully added into DB for tr: %s" % prm['id'])
        return True
    except psycopg2.Error as e:
        print('e', e)
        logging.info("add_param_to_db(prm) ended up with error: %s" % e)
        return False


def download_file_auth(im_p, suf, url, id, user, passwd):
    """
    Download file from web by requests
    :param url: Url of image
    :param user: https_username
    :param passwd: https_password
    :return: If downloaded return True else return False
    """
    auth = HTTPBasicAuth(user, passwd)
    im_name = id + '.' + suf
    im_path = os.path.join(im_p, im_name)
    try:
        r = requests.get(url=url, verify=False)
        if r.status_code == 401:
            r = requests.get(url=url, auth=auth, verify=False)
        with open(im_path, 'wb') as f:
            f.write(r.content)
        if os.path.exists(im_path):
            return True
    except requests.exceptions.RequestException as e:  # This is the correct syntax
        logging.info("requests.get() ended up with error: %s" % e)
        return False



if __name__ == '__main__':

    # Get command line arguments
    parser = argparse.ArgumentParser(description="Creating first key-value storage")
    parser.add_argument('-i', '--url', type=str, required=True,
                       help="Transient's URL")
    args = parser.parse_args()
    url = args.url

    # Get main_page
    parsed_url = urlsplit(url)
    main_page = 'https://' + parsed_url.netloc + '/' + parsed_url.path.split('/')[1] + '/'
    print('main_page', main_page)

    # Get transient id
    query_dict = dict(parse_qsl(parsed_url.query))
    trid = query_dict['id']
    print('trid', trid)

    # Check If the transient ID already exists in DB
    check_id = check_trid_indb(trid)
    print('check_id', check_id)

    if int(check_id) == 1:
        e = 'The transient ID: ' + trid + ' already exist in DB and files downloaded'
        logging.info("download_images.py ended up with INFO: %s" % e)
    elif int(check_id) == 2 or int(check_id) == 3:
        # Get HTML page source code
        text_data = get_html(url, uname, pswd)

        # Get <frame src="prev.php?xc=> or <frame src="traadd.php tags
        frames = get_frame(text_data)

        # Extract frame 'trm.php' from frames
        # For T
        tr_params_frame = [frame for frame in frames if 'trm' == frame[:3]]
        im_frames = [frame for frame in frames if 'trm' != frame[:3]]
        # For I
        # tr_params_frame = [frame for frame in frames if 'trans_m' == frame[:7]]
        # im_frames = [frame for frame in frames if 'trans_m' != frame[:7]]

        tr_params_data = get_html(main_page + tr_params_frame[0])
        tr_params = get_tr_params(trid, tr_params_data)

        # Get transient datetime And Create dir
        dir_name = create_dir(im_dir, tr_params)
        tr_params['path'] = dir_name

        # Get Observatory ID for tr_param dict
        obs_id = get_obsid(url, cfg_path)
        tr_params['obs_id'] = obs_id

        im_urls = []
        for frame in im_frames:
            # print(main_page + frame)
            im_data = get_html(main_page + frame)
            # Get list of urls for final images for each transients to download
            urls_from_frame = get_img_url(im_data, main_page)

            im_urls.extend(urls_from_frame)

        # download 8 images from im_urls with different siffix
        suffs = ['tr.jpeg', 'dss_search.gif', 'sub.jpeg', 'sdss.jpeg',
                 'second_lap.jpeg', 'max_limit.jpeg', 'log.jpeg', 'early.jpeg']


        # If there is not subtraction in preview, the indexes of list im_urls shift to the left
        # In this case we can insert empty element to index 2
        flag = None
        for im_url in im_urls:
            prs_url = urlsplit(im_url)
            query_dict = dict(parse_qsl(prs_url.query))
            if 'cat' in query_dict.keys() and 'sub' == query_dict['fits'].split('/')[-1].split('.')[0]:
                flag = 1
                break
        if not flag:
            im_urls.insert(2, '')



        # Parse Image URL AND Download it with specific suffix
        tr_prodata_id = ''  # To make a different between SecondLap URL  and MaxLimit URL
        for im_url in im_urls:
            prs_url = urlsplit(im_url)
            query_dict = dict(parse_qsl(prs_url.query))
            # Transient
            if im_url == '':    # There is not image in preview and we put empty url
                tr_params['tr'] = False
            elif 'cat' in query_dict.keys() and 'sub' != query_dict['fits'].split('/')[-1].split('.')[0]:
                tr_prodata_id = query_dict['fits'].split('/')[-2]
                # True if image has been downloaded successfully
                tr_params['tr'] = download_file_auth(dir_name, suffs[0], im_url, trid, uname, pswd)
            # DSS
            elif 'arc' == prs_url.netloc.split('.')[0][:3]:
                tr_params['dss'] = download_file_auth(dir_name, suffs[1], im_url, trid, uname, pswd)
            else:
                tr_params['early'] = download_file_auth(dir_name, suffs[7], im_url, trid, uname, pswd)
        # Can be a case, when there is not url for subtrction in transient Page
        # In this case set tr_params['sub'] = False
        try:
            _ = tr_params['sub']
        except KeyError:
            tr_params['sub'] = False

        if int(check_id) == 2:
            # There is id, but there is not transient image's path (tr != True)
            # ADD new params values
            print('tr_params', tr_params)
            add_params = add_param_to_db(tr_params)
            print('add_params', add_params)
        else:
            # check_id == '3' There is NOT id, and there is NOT transient image's path
            # Insert All Params to DataBase
            print('tr_params', tr_params)
            insert_params = insert_param_to_db(tr_params)
            print('insert_params', insert_params)
    else:
        e = 'The transient ID: ' + trid + ' already exist in DB and files downloaded'
        logging.info("download_images.py ended up with INFO: %s" % e)




# TESTS

def test_create_dir():
    tr_params =  {'id': '30215426', 'datetime': '2020-06-20 04:09:35.189', 'coord2000': '22h 08m 40.35s  -57d 26m 26.0s ',
     'mag': '16.26', 'Band': 'W', 'Limit': '19.12', 'flux': '16752.9', 's/n': '76.4', 'xc': '779.6', 'yc': '3255.99',
     'fwhm': '7.5', 'a': '1.4', 'b': '1.1', 'PA': '10.52', 'N': '', 'C': '1', 'Gal': '   | NGC7205 ', 'd_ra': '48.8E',
     'ddec': '7.3N', 'dmag': '  0.0  ', 'User': '    pogrosheva '}
    wd = 'wd'
    true_path = '/wd/2020/06/20/30215426'
    res_path = create_dir(wd, tr_params)
    print('Test #1:', "OK" if true_path == res_path else "Failed")

print()
test_create_dir()
