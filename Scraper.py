#!python3
# -*- coding:utf-8 -*-
'''
  WeatherScraper
    要件：
 　   日本国気象庁のホームページから気象データを採集する。
      採集したデータはcsvへと出力する。
    仕様技術：
    　１）BeautifulSoupでスクレイピングを行う。
    　２）pandasでデータ出力を行う。
    仕様詳細：
      １）
      　　・指定された範囲内の年月日で情報を取得する。 (fromDateからtoDateまで)
            範囲内の年月日の数だけBeautifulSoupを繰り返し実行する。
          ・1時間ごとの値を取得する (hourly_s1.phpをリクエストする)
            データ１レコード＝１時間ごとのデータ　となる。
          ・県番号(prec_no)と地区番号(block_no)も指定可能としておく。
         ・URLは変更する意味がないので固定する。
          ・取得情報については固定とする。特定の項目のみに絞り込む機能は仕様対象外とする。
      ２）
          ・県番号、地区番号、日付、時刻で昇順ソートする。
          ・検索が全て完了した後にCSVに出力する。
      ３）その他
      　　ログは実行場所の直下にWeatherScraper.logとして出力される。
    呼び出し元モジュールからの用法：
      １）当モジュールをインポートする
      ２）scraping関数を実行する。引数には範囲年月日を指定する。
          dateFormat変数に指定されたフォーマットに合わせること。
      ３）csvName変数に指定された場所およびファイル名で出力される。
    mainscriptとしての用法：
       以下のようにコマンドとして実行する。
          python Scraper.py 開始日付 終了日付
'''
import sys
from time import sleep
import traceback
from datetime import datetime, timedelta
import logging, logging.config
import requests
from bs4 import BeautifulSoup
import pandas as pd

'''
  モジュール用の変数
'''
#URL固定
target_url = r'http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php'
#日付のフォーマット
dateFormat = '%Y-%m-%d'
#出力ファイル名 csv
csvName = 'hourly_s1.csv'
#１リクエスト毎の待機秒数
#サイトへの処理負荷低減のため
waitTime = 0.5
#requestsのタイムアウト設定
connTimeout = 1.0
readTimeout = connTimeout

#ロガー
__mylogger = logging.getLogger('ScraperLogger')

def __validationCheck(fr, to):
    '''
      入力値とメンバ変数のチェック
    '''
    
    #日付文字列を日付型へ変換する
    try:
        fr = datetime.strptime(fr, dateFormat)
        to = datetime.strptime(to, dateFormat)
    except Exception as ex:
        #変換に失敗したら処理を中断する
        __mylogger.error('Format of date arguments is wrong.')
        return False
    
    #日付範囲の大小チェック
    if fr > to:
        __mylogger.error('fromDate is bigger than toDate. Arguments is wrong!')
        return False
    
    return True

def scraping(fromDate, toDate, prec_no=91, block_no=47936):
    '''
      スクレイピングを行うメイン処理
    '''
    
    #所要時間計測用
    startTime = datetime.now()
    __mylogger.info('scraping start!')
    
    #入力パラメータの妥当性チェックを行う
    #TODO: prec_noとblock_noは仕様が分かり次第チェック処理を加える
    if __validationCheck(fromDate, toDate) == False:
        __mylogger.error('invalid arguments.')
        return False
    
    #日付範囲の日数を計算する。日数分のリクエストを発行させるために使う
    fromDateTime = datetime.strptime(fromDate, dateFormat)
    toDateTime = datetime.strptime(toDate, dateFormat)    
    amountDate = toDateTime - fromDateTime
    
    #データレコードを格納するリスト
    dataRec = []
    
    #日数分のリクエストを発行する
    for daynum in range(0, amountDate.days + 1):
        try:
            day = fromDateTime + timedelta(daynum)
            
            #１日の情報を取得する
            page = requests.get(target_url + '?prec_no={0}&block_no={1}&year={2}&month={3}&day={4}&view=p1'.format(prec_no, block_no, day.year, day.month, day.day), \
                                timeout=(connTimeout, readTimeout))
            page.raise_for_status()
            soup = BeautifulSoup(page.content, 'html.parser')
            
            #１時間ごとの情報を取得する
            #ただし先頭２行目まではヘッダ部なので読み飛ばす。
            rows = soup.select('table#tablefix1 tr')
            for i in range(2, len(rows)):
                columns = rows[i].select('td')
                
                #データの取得
                # 天気だけはテキストノードではなくaltに値があったのでそこから取る。
                col14Val = columns[14].find('img')
                if col14Val is not None and col14Val.get('alt') is not None:
                    col14Val = col14Val.get('alt')
                    
                #データの取得
                dataRec.append([prec_no,
                                block_no,
                                day.strftime(dateFormat),
                                int(columns[0].text), 
                                columns[1].text, 
                                columns[2].text, 
                                columns[3].text, 
                                columns[4].text, 
                                columns[5].text, 
                                columns[6].text, 
                                columns[7].text, 
                                columns[8].text, 
                                columns[9].text, 
                                columns[10].text,
                                columns[11].text, 
                                columns[12].text, 
                                columns[13].text,  
                                col14Val,
                                columns[15].text, 
                                columns[16].text])
            
            #処理負荷をかけ過ぎないように、１リクエストごとに一定時間待機させる
            sleep(waitTime)
            
        except Exception as ex:
            #所要時間
            __mylogger.error('exception has occured')
            __mylogger.error(ex.__class__.__name__)
            __mylogger.error(traceback.format_exc())
        
    #pandasでcsv出力する
    #ヘッダ情報
    header = ['県番号', '地区番号', '日付', '時刻', '気圧・現地(hPa)', '気圧・海面(hPa)', '降水量', '気温(C)', '露天温度(C)', '蒸気圧(hPa)', '湿度(%)', '風速(m/s)', '風向', '日照時間(h)', '全天日射量(MJ/m2)', '降雪(cm)', '積雪(cm)', '天気', '雨雲', '視程(km)' ]
    df = pd.DataFrame(dataRec, columns=header)
    df = df.sort_values(by=['県番号', '地区番号', '日付', '時刻'], ascending=True)
    df.to_csv(csvName, index=False)
    
    #所要時間計測用
    endTime = datetime.now()
    __mylogger.info('scraping ended')
    
    #所要時間
    __mylogger.info('amount of time::' + str(endTime - startTime))

if __name__ == '__main__':
    #テスト実行時の設定
    logging.config.fileConfig(fname='logsetting.conf', disable_existing_loggers=False)
    scraping(sys.argv[1], sys.argv[2])
else:
    logging.config.fileConfig(fname='logsetting.conf', disable_existing_loggers=False)
