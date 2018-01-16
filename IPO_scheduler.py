from __future__ import print_function
import httplib2
import os
import datetime
import logging
import requests
import pandas as pd
import re

from bs4 import BeautifulSoup
from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

#If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/calendar-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/calendar'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'IPO_Scheduler'

def setup_logger():
    """
    Setup Logger.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    log_file_path = os.path.join(os.getcwd(), 'logs')
    log_file_path = os.path.join(log_file_path, 'calendarInsertEvent.log')
    fh = logging.FileHandler(log_file_path)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    logger.addHandler(sh)

    formatter = logging.Formatter(
        '%(asctime)s:%(lineno)d:%(levelname)s:%(message)s')
    fh.setFormatter(formatter)
    sh.setFormatter(formatter)

    return logger

logger = setup_logger()

def scraping():
    url='https://www.sbisec.co.jp/ETGate/?OutSide=on&_ControlID=WPLETmgR001Control&_DataStoreID=DSWPLETmgR001Control&burl=search_domestic&dir=ipo%2F&file=stock_info_ipo.html&cat1=domestic&cat2=ipo&getFlg=on'
    res = requests.get(url)
    content = res.content
    soup = BeautifulSoup(content, 'html.parser')
    #DataFrameの作成
    df = pd.DataFrame(columns=['companyname', 'bookbill_term', 'conditional_determined_date', 'price', 'unit', 'listing_date'])
    #会社名を記述するリストを作成
    company_list = []
    #IPO予定の会社名リストを取得
    companies = soup.find_all('div', class_='thM alL')
    for company in companies:
        name = company.find('p', class_='fl01').text
        company_list.append(name)
    #companynameカラムに情報を入力
    df.loc[:, 'companyname'] = company_list

    #各会社のIPO情報を取得
    informations = soup.find_all('div', class_='accTbl01')
    i = 0
    for information in informations:
        #IPO情報を記述するレコードを作成
        info_list = []
        div_info = information.find_all('div', class_='tdM')
        for div in div_info:
            try:
                info = div.find('p', class_='fm01').text
                info_list.append(info)
            #valueがnoneとなる抽出対象はtextアトリビュートでエラーとなるので、例外をpassする。
            except:
                pass
        #1社ごとに情報をDataFrameへ反映する。
        df.iloc[i, 1:5] = info_list
        #次の行へ書き出す為にカウントアップ
        i = i +1
    #csvファイルへ書き出し
    return df

def format_csv(df):
    csv_file_dir = os.path.join(os.getcwd(), 'csvfiles')
    csv_file_path = os.path.join(csv_file_dir, 'output.csv')
    old_csv_file = os.path.join(csv_file_dir, 'output_old.csv')
    temp_csv_path = os.path.join(csv_file_dir, 'temp.csv')
        #旧ファイルの削除
    if os.path.exists(old_csv_file):
        os.remove(old_csv_file)
    else:
        pass
        #csvファイルの世代管理
    if os.path.exists(csv_file_path):
        os.rename(csv_file_path,  old_csv_file)
    else:
        pass
    #書込用ファイルの作成
    f = open(csv_file_path, 'a')
    f.close()
    #scraping()で取得したdfをcsv書き出し
    df.to_csv(csv_file_path)
    #新旧比較し重複分を除いたDataFrameを作成する
    current_df = pd.read_csv(csv_file_path, index_col=0)
    if os.path.exists(old_csv_file):
        old_df = pd.read_csv(old_csv_file, index_col=0)
        #旧世代のcsvファイルからcompanynameカラムの値をリストとして取得
        old_list = old_df.loc[:, 'companyname'].values
        #old_list内の値と一致するcompanynameを持つ行を除外する
        for company_name in old_list:
            current_df = current_df[~current_df['companyname'].str.contains(company_name)]
    else:
        pass
    current_df.to_csv(temp_csv_path)
    return temp_csv_path

def get_credentials():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.
cd
    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'calendar-python-quickstart.json')

    store = Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def create_api_body(values):
    now = datetime.datetime.now()
    #開始日時フォーマットの整形
    start_time = values['bookbill_term'].split('〜')[0]
    start_time = re.split('[^0-9/:\s:]', start_time)[0]
    start_time = str(now.year) + '/' + start_time
    start_time = start_time.replace('/', '-')
    start_time = start_time.replace(' ', 'T', 1)
    start_time = start_time + ':00+09:00'
    #終了日時フォーマットの整形
    end_time = values['bookbill_term'].split('〜')[1]
    end_time = re.split('[^0-9/:\s:]', end_time)[0]
    end_time = str(now.year) + '/' + end_time
    end_time = end_time.replace('/', '-')
    end_time = end_time.replace(' ', 'T', 1)
    end_time = end_time.replace(' ', '')
    end_time = end_time + ':00+09:00'

    summary = '[IPO]' + values['companyname']
    body = {
        'summary': summary,
        'start': {
            'dateTime': start_time,
            'imeZone': 'Asia/Tokyo',
        },
        'end': {
            'dateTime': end_time,
            'imeZone': 'Asia/Tokyo',
        },
        'reminders': {
            'useDefault': False,
            'overrides': [
            {'method': 'email', 'minutes': 24 * 60},
            {'method': 'popup', 'minutes': 15},
            ],
        },
    }
    return body

def main():
    """Shows basic usage of the Google Calendar API.

    Creates a Google Calendar API service object and outputs a list of the next
    10 events on the user's calendar.
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    service = discovery.build('calendar', 'v3', http=http)

    df = scraping()
    temp_csv_path = format_csv(df)
    #csvを読み込んでdf作成
    event_df = pd.read_csv(temp_csv_path, index_col=0)

    for i, values in event_df.iterrows():
        try:
            #予定csv件数繰り返し
            body = create_api_body(values)
            #API呼び出し
            event = service.events().insert(calendarId='primary', body=body).execute()
            if not event.get('htmlLink'):
                logger.error('Failed create event.')
            else:
                logger.info("Event created : %s" % event.get('htmlLink'))

        except Exception as e:
            logger.exception(e)

    #イベント作成用csvファイルの削除
    os.remove(temp_csv_path)

if __name__ == '__main__':
    main()
