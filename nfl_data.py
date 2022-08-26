import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os
import lxml
import time
import concurrent.futures
import threading
import shutil

years = [_ for _ in range(1970, 2022)]
directories = 0


class GetInfo():
    thread_local = threading.local()
    requests_session = requests.Session()
    done = 0
    years = [_ for _ in range(1970, 2022)]
    core_url = 'https://www.nfl.com/stats/player-stats/category/{category_key}/{year}/pre/all/{category_val}/desc'
    categories = {
        'passing': 'passingyards',
        'rushing': 'rushingyards',
        'recieving': 'receivingreceptions',
        'fumbles': 'defensiveforcedfumble',
        'tackles': 'defensivecombinetackles',
        'interceptions': 'defensiveinterceptions',
        'field-goals': 'kickingfgmade',
        'kickoffs': 'kickofftotal',
        'kickoff-returns': 'kickreturnsaverageyards',
        'punts': 'puntingaverageyards',
        'punt-returns': 'puntreturnsaverageyards'
    }
    links_to_parse = []

    def get_session(self):
        if not hasattr(self.thread_local, "session"):
            self.thread_local.session = requests.Session()
            print('Created session')
        return self.thread_local.session

    def make_dir(self):
        os.mkdir('nfl_data')
        os.mkdir('nfl_data/players')
        os.mkdir('nfl_data/players/years')
        for year in self.years:
            try:
                os.mkdir(f'nfl_data/players/years/{year}')
                for category in self.categories:
                    os.mkdir(f'nfl_data/players/years/{year}/{category}')
                    print(f'Created directoty nfl_data/players/years{year}/{category}')
            except Exception as e:
                print('Error')
                print(e)
                continue
            finally:
                print('Succeed')
                print(f'Directories created: {len(self.years)}')

    def remove_dir(self):
        shutil.rmtree('nfl_data')
        print('removed')

    def url_generate(self):
        global directories
        for year in self.years:
            for category in self.categories:
                url = self.core_url.format(category_key=category, category_val=self.categories[category], year=year)
                self.links_to_parse.append(url)
                directories += 1
        return self.links_to_parse

    def get_col_names(self, url):
        page = self.requests_session.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        cols_names = []
        cols = soup.find_all('th', scope='col')
        for col in cols:
            cols_names.append(col.text)
        return cols_names

    def get_names(self, url):
        page = self.requests_session.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        names_list = []
        names = soup.find_all('div', class_='d3-o-media-object__body')
        for name in names:
            names_list.append(name.text)
        return names_list

    def get_data(self, url):
        page = self.requests_session.get(url)
        soup = BeautifulSoup(page.content, 'lxml')
        data_nested_list = []
        tb = soup.find('tbody')
        data_list = tb.find_all('td')
        cols_len = len(self.get_col_names(url))

        cnt = 0
        loc_list = []
        for data in data_list:
            if not re.search('[A-Za-z]', data.text):
                loc_list.append(data.text)
            cnt += 1

            if cnt == cols_len + 1:
                data_nested_list.append(loc_list)
                loc_list = []
                cnt = 0

        return data_nested_list

    def get_all_data(self, url):

        link = url
        res = {'names': [], 'col_names': [], 'data': []}
        names_list = []
        cols_names = []
        data_nested_list = []
        is_not_last = True

        def data_collector(url):
            nonlocal is_not_last
            nonlocal link
            start_request = time.time()
            session = self.get_session()
            page = session.get(url)
            time.sleep(0.1)
            soup = BeautifulSoup(page.content, 'lxml')
            end_request = time.time() - start_request
            print(f'Time for request {url}: \n {end_request}')

            names = soup.find_all('div', class_='d3-o-media-object__body')
            for name in names:
                names_list.append(name.text)

            cols = soup.find_all('th', scope='col')
            for col in cols:
                if col.text not in cols_names:
                    cols_names.append(col.text)
            tb = soup.find('tbody')
            data_list = tb.find_all('td')
            cols_len = len(cols_names)

            cnt = 0
            loc_list = []
            for data in data_list:
                if not re.search('[A-Za-z]', data.text):
                    loc_list.append(data.text)
                cnt += 1

                if cnt == cols_len + 1:
                    data_nested_list.append(loc_list)
                    loc_list = []
                    cnt = 0

            is_not_last = soup.find('a', class_='nfl-o-table-pagination__next').has_attr('href')
            print(f'Is not last: {is_not_last}')
            link = 'http://www.nfl.com' + soup.find('a', class_='nfl-o-table-pagination__next')['href']

        iteration = 0
        try:
            while is_not_last:
                data_collector(link)
                print(f'iterationL {iteration}')
                iteration += 1
        finally:

            for data, name in zip(data_nested_list, names_list):
                data.insert(0, name)
            cols_names.insert(0, 'Players')

            res['names'] = names_list
            res['data'] = data_nested_list
            res['col_names'] = cols_names

            names_len = len(res['names'])
            res['data'] = res['data'][:names_len]
            return res

    def get_data_frame(self, url):
        all_data = self.get_all_data(url)
        df_start_time = time.time()
        data_list = all_data['data']
        names = all_data['names']
        col_names = all_data['col_names']

        data_frame = pd.DataFrame(columns=col_names, index=range(len(names)))

        for i, data in zip(range(len(names)), data_list):
            dictionary = {}
            for val, col in zip(data, col_names):
                dictionary[col] = val
            data_frame.loc[i] = pd.Series(dictionary)
        df_end_time = time.time() - df_start_time
        print(f'Time for df: {df_end_time}')
        return data_frame

    def make_csv(self, urls):
        global done
        print('triggered')
        name_pattern = r'(category)\/[a-z-]*\/[0-9]{4}'

        try:
            self.make_dir()
        except Exception as e:
            print(e)

        data = re.search(name_pattern, urls).group().replace('/category', '').split('/')
        name = re.search(name_pattern, urls).group().replace('category/', '').replace('/', '-') + '.csv'
        address = f'nfl_data/players/years/{data[2]}/{data[1]}'
        print(f'{address}/{name}')
        try:
            df = GetInfo().get_data_frame(urls)
            start_to_csv = time.time()
            df.to_csv(f'{address}/{name}', index=True)
            end_to_csv = time.time() - start_to_csv
            print(f'Time for csv: {end_to_csv}')

            print('=' * 20)
            self.done += 1
            print(f'Created {address}/{name}')
            print(f'{self.done / directories * 100}% done')
        except Exception as e:
            print(';' * 20)
            print(e)
            print(';' * 20)

    def generate_database(self):
        url = self.url_generate()
        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(self.make_csv, url)


GetInfo().remove_dir()
