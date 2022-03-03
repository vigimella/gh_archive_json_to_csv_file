# Imports section

import gzip, json, os
import multiprocessing
import calendar
import pandas as pd

from re import search
from datetime import datetime

import logging as log

log.basicConfig(level=log.INFO, format='%(asctime)s :: proc_id %(process)s :: %(funcName)s :: %(levelname)s :: %(message)s')

# Folder abs paths 

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
gh_archive_csv_dir = os.path.join(APP_ROOT, 'gh_archive_csv')
zip_files_dir = os.path.join(APP_ROOT, 'gh_archive_zip_files')


# Function definition


def multiple_unzip_file(elms_to_find, web_url):
    """
    This function allows to unzip files previously downloaded and to create a csv files
    """

    a = web_url

    a = a.replace('https://data.gharchive.org/', '')
    folder_name = a.replace('.json.gz', '')
    log.info(folder_name)

    folder_path = os.path.join(zip_files_dir, folder_name)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    log.info(f'Dowloading {web_url}...')
    os.system(f'wget -q -o /dev/null {web_url} -P {folder_path}')

    zip_files = []
    file_names = []

    # Add all files in folder path in list with abs path in zip_files and add all files name in file_names list.

    for file in os.listdir(folder_path):
        if file.endswith('.json.gz'):
            filename = file.replace('.json.gz', '')
            file = os.path.join(folder_path, file)

            zip_files.append(file)
            file_names.append(filename)

    # Extract all files in list 

    for i, file in enumerate(zip_files):
        k = 0
        types = list()
        push_events = list()
        rows = list()

        # optional information could be removed, it is just a statistical output

        # log.info('Optional information:')

        with gzip.open(file) as f:
            for line in f:
                json_data = json.loads(line)
                types.append(json_data['type'])
                data_frame = pd.DataFrame({"types": types}).groupby("types").size().sort_values(ascending=False)
            # log.info(data_frame)

        # log.info('CSV creation...')

        log.info(f'Processing {file}...')
        with gzip.open(file) as archive:

            for line in archive:

                json_data = json.loads(line)

                if json_data['type'] == 'PushEvent':
                    push_events.append(json_data)

            rows = list()

            for push in push_events:

                repo_name = push['repo']['name']
                commit_date = push['created_at']

                for commit in push['payload']['commits']:

                    if commit['distinct']:

                        # print('Searching if in a commit message exists a word to find \n')

                        for find in elms_to_find:

                            if search(find, commit['message']):
                                log.info(f'Something has been found: commit={commit["sha"]},repo={repo_name}')

                                rows.append({

                                    'repository': repo_name,
                                    'author': commit['author']['name'],
                                    'commit_message': commit['message'],
                                    'commit_id': commit['sha'],
                                    'commit_date': commit_date

                                })

                    commits_data = pd.DataFrame(rows)

                    csv_name = 'gh_archive_' + str(file_names[k]) + '.csv'
                    csv_location_path = os.path.join(gh_archive_csv_dir, csv_name)

                commits_data.to_csv(csv_location_path, sep=',', encoding='utf-8')

        log.info(f'CSV {csv_location_path} created')


def url_generation(start_date, end_date):
    # date creation using date_range by pandas

    dates = pd.date_range(start_date, end_date)

    # adding dates into a list called new_dates

    new_dates = []

    for date in dates:
        # going to replace 00:00:00 with H used as placeholder
        new_dates.append(str(date).replace(' 00:00:00', '') + '-H')

    # adding hour to date

    for date in new_dates:
        for i in range(24):
            URLs.append('https://data.gharchive.org/' + date.replace('-H', '-' + str(i)) + '.json.gz')


def multiprocess(URLs, elms_to_find):

    for url in URLs:
        multiple_unzip_file(elms_to_find, url)


if __name__ == '__main__':

    # Check if the directories already exist or not

    if not os.path.exists(gh_archive_csv_dir):
        os.makedirs(gh_archive_csv_dir)
        log.info('Directory gh_archive_csv_dir created...')

    if not os.path.exists(zip_files_dir):
        os.makedirs(zip_files_dir)
        log.info('Directory zip_files_dir created...')

    # The find variable is the filter to apply to the mining

    elms_to_find = ['diff privacy', 'differential privacy', 'd privacy', 'dif. privacy', 'dp', 'differential priv.',
                    'diff priv.']
    URLs = []

    # dates generation

    start_date = '2020-01-01' # yyyy-mm-dd
    end_date = '2021-12-31' # yyyy-mm-dd

    url_generation(start_date, end_date)

    # Splitting URLs list in sublist

    len_list = len(URLs)
    n = int(len_list / 4)

    # using list comprehension

    final_urls = [URLs[i * n:(i + 1) * n] for i in range((len(URLs) + n - 1) // n)]

    # Multiprocessing

    p1 = multiprocessing.Process(target=multiprocess, args=(final_urls[0], elms_to_find))
    p2 = multiprocessing.Process(target=multiprocess, args=(final_urls[1], elms_to_find))
    p3 = multiprocessing.Process(target=multiprocess, args=(final_urls[2], elms_to_find))
    p4 = multiprocessing.Process(target=multiprocess, args=(final_urls[3], elms_to_find))

    # Start multiprocessing

    p1.start()
    p2.start()
    p3.start()
    p4.start()
