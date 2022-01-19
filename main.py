# Imports section

import gzip, json, os
import multiprocessing
import calendar
import pandas as pd

from re import search
from datetime import datetime

# Folder abs paths 

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
gh_archive_csv_dir = os.path.join(APP_ROOT, 'gh_archive_csv')
zip_files_dir = os.path.join(APP_ROOT, 'gh_archive_zip_files')


# Function definition

# This function allows to unzip files previously downloaded and to create a csv files

def multiple_unzip_file(elms_to_find, web_url):
    a = web_url

    a = a.replace('https://data.gharchive.org/', '').split('-')
    folder_name = str(a[0]) + '-' + str(a[1])
    print(folder_name)

    folder_path = os.path.join(zip_files_dir, folder_name)

    if not os.path.exists(folder_path):
        os.makedirs(folder_path)

    os.system(f'wget {web_url} -P {folder_path}')

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

        print('Optional information: \n')

        with gzip.open(file) as f:
            for line in f:
                json_data = json.loads(line)
                types.append(json_data['type'])
                data_frame = pd.DataFrame({"types": types}).groupby("types").size().sort_values(ascending=False)
            print(data_frame)

        print('CSV creation... \n')

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

                        print('Searching if in a commit message exists a word to find \n')

                        for find in elms_to_find:

                            if search(find, commit['message']):

                                print('Something has been found \n')

                                rows.append({

                                    'repository': repo_name,
                                    'author': commit['author']['name'],
                                    'commit message': commit['message'],
                                    'commit id': commit['sha'],
                                    'commit_date': commit_date

                                })

                    commits_data = pd.DataFrame(rows)

                    csv_name = 'gh_archive_' + str(file_names[k]) + '.csv'
                    csv_location_path = os.path.join(gh_archive_csv_dir, csv_name)

                commits_data.to_csv(csv_location_path, sep=',', encoding='utf-8')

        print('CSV n. ' + str(k + 1) + ' created...')

        k = k + 1


# Generation of URL to download .json.gz file

def url_generation(year, month, day, hour):
    current_date = datetime.now().strftime('%Y-%m-%d-%H')
    date_to_anlyze = str(year) + '-' + str(month) + '-' + str(day) + '-' + str(hour)

    if date_to_anlyze <= current_date:

        if month < 10 and day < 10:

            # url: 2020-01-01 
            new_url = 'https://data.gharchive.org/' + str(year) + '-0' + str(month) + '-0' + str(day) + '-' + str(
                hour) + '.json.gz'

        elif month < 10 and day > 9:

            # url: 2020-01-10
            new_url = 'https://data.gharchive.org/' + str(year) + '-0' + str(month) + '-' + str(day) + '-' + str(
                hour) + '.json.gz'

        elif month > 9 and day > 9:

            # url: 2020-10-10
            new_url = 'https://data.gharchive.org/' + str(year) + '-' + str(month) + '-' + str(day) + '-' + str(
                hour) + '.json.gz'

        elif month > 9 and day < 10:

            # url: 2020-10-01
            new_url = 'https://data.gharchive.org/' + str(year) + '-' + str(month) + '-0' + str(day) + '-' + str(
                hour) + '.json.gz'

        URLs.append(new_url)

    # Multiprocess function definition


def multiprocess(years, months, hours, elms_to_find):
    for year in years:

        for month in months:

            num_days = calendar.monthrange(year, month)[1]

            for day in range(1, num_days):

                for hour in hours:
                    url_generation(year, month, day, hour)

    for url in URLs:
        multiple_unzip_file(elms_to_find, url)


if __name__ == '__main__':

    # Check if the directories already exist or not

    if not os.path.exists(gh_archive_csv_dir):
        os.makedirs(gh_archive_csv_dir)
        print('Directory gh_archive_csv_dir created...')

    if not os.path.exists(zip_files_dir):
        os.makedirs(zip_files_dir)
        print('Directory zip_files_dir created...')

    # The find variable is the filter to apply to the mining

    elms_to_find = ['diff privacy', 'differential privacy', 'd privacy', 'dif. privacy', 'dp', 'differential priv.', 'diff priv.']
    URLs = []

    # Months are divided by trimester

    months_first = [1, 2, 3]
    months_second = [4, 5, 6]
    months_third = [7, 8, 9]
    months_fourth = [10, 11, 12]

    # Years to analyze

    years = [2020, 2021]

    # Hours to analyze

    hours = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]

    # Multiprocessing

    p1 = multiprocessing.Process(target=multiprocess, args=(years, months_first, hours, elms_to_find))
    p2 = multiprocessing.Process(target=multiprocess, args=(years, months_second, hours, elms_to_find))
    p3 = multiprocessing.Process(target=multiprocess, args=(years, months_third, hours, elms_to_find))
    p4 = multiprocessing.Process(target=multiprocess, args=(years, months_fourth, hours, elms_to_find))

    # Start multiprocessing

    p1.start()
    p2.start()
    p3.start()
    p4.start()
