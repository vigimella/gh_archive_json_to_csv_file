# Imports section

import gzip, json, os, re, random, shutil, time, sys
import multiprocessing
import pandas as pd

from concurrent.futures import ThreadPoolExecutor

import logging as log

log.basicConfig(level=log.INFO,
                format='%(asctime)s :: proc_id %(process)s :: %(funcName)s :: %(levelname)s :: %(message)s')

# Folder abs paths 

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
gh_archive_csv_dir = os.path.join(APP_ROOT, 'gh_archive_csv')
zip_files_dir = os.path.join(APP_ROOT, 'gh_archive_zip_files')


def clean_directory(dir_path):
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


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
        for i in range(5):
            URLs.append('https://data.gharchive.org/' + date.replace('-H', '-' + str(i)) + '.json.gz')


def download_json_file(web_url):
    file_name = web_url.replace('https://data.gharchive.org/', '')
    file_name = file_name.replace('.json.gz', '')
    log.info(file_name)

    log.info(f'Downloading {web_url}...')
    os.system(f'wget -q -o /dev/null {web_url} -P {zip_files_dir}')
    log.info('Download finished...')


def csv_creation(file):
    log.info(f'Analyzing file -> {file}')

    types = list()
    push_events = list()
    rows = list()

    # file = os.path.join(zip_files_dir, file)

    log.info(f'File path is: {file}')

    file_name = file.replace(zip_files_dir, '')
    file_name = file_name.replace('.json.gz', '')
    file_name = file_name.replace('/', '')

    log.info(f'File name --> {file_name}')

    with gzip.open(file) as f:
        for line in f:
            json_data = json.loads(line)
            types.append(json_data['type'])
            data_frame = pd.DataFrame({"types": types}).groupby("types").size().sort_values(ascending=False)
        # log.info(data_frame)

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

                    if re.search('diff.* pri.*', commit['message']):
                        log.info(f'Something has been found: commit={commit["sha"]},repo={repo_name}')
                        rows.append({

                            'repository': repo_name,
                            'author': commit['author']['name'],
                            'commit_message': commit['message'],
                            'commit_id': commit['sha'],
                            'commit_date': commit_date,
                            'url': 'https://github.com/' + repo_name

                        })

                commits_data = pd.DataFrame(rows)

                csv_name = 'gh_archive_' + file_name + '.csv'
                csv_location_path = os.path.join(gh_archive_csv_dir, csv_name)

            commits_data.to_csv(csv_location_path, sep=',', encoding='utf-8')

    log.info(f'CSV {csv_location_path} created')


if __name__ == '__main__':

    # Check if the directories already exist or not

    if not os.path.exists(gh_archive_csv_dir):
        os.makedirs(gh_archive_csv_dir)
        log.info('Directory gh_archive_csv_dir created...')

    if not os.path.exists(zip_files_dir):
        os.makedirs(zip_files_dir)
        log.info('Directory zip_files_dir created...')

    clean_directory(gh_archive_csv_dir)
    log.info(f'{gh_archive_csv_dir} cleaned...')
    clean_directory(zip_files_dir)
    log.info(f'{zip_files_dir} cleaned...')

    URLs = []
    JSONs = []

    N_THREAD = 4

    # dates generation

    # start_date = '2020-01-01'  # yyyy-mm-dd
    start_date = '2020-07-05'  # yyyy-mm-dd
    # end_date = '2021-12-31'  # yyyy-mm-dd
    end_date = '2020-07-05'  # yyyy-mm-dd

    url_generation(start_date, end_date)

    for elm in os.listdir(zip_files_dir):
        if elm.endswith('.json.gz'):
            elm = os.path.join(zip_files_dir, elm)
            JSONs.append(elm)

    with ThreadPoolExecutor(N_THREAD) as p:
        p.map(download_json_file, URLs)

    with ThreadPoolExecutor(N_THREAD) as p:
        p.map(csv_creation, JSONs)
