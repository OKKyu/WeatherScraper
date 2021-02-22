#!python3
# -*- coding:utf-8 -*-
'''
  WeatherDao
    要件：
 　   日本国気象庁のホームページから採集した気象データをデータベースで保管する。
      データの出力・集計機能も提供する。
    仕様技術：
    　１）データベースにsqliteを用いる。
      ２）データを内部で操作・管理するためにpandasを用いる。
    仕様詳細：
      １）入力仕様
      　　・csvを入力対象とする。先頭１行目はヘッダレコードで、２行目以降はデータレコードとする。
          ・項目は気象庁ホームページの項目と同じとする。また、テーブル定義書は別途資料に記載する。
          ・入力対象は内部でpandasのDataFrameへ変換する。もしくはpandasで直接csvを読み込む。
      ２）内部処理仕様
          ・入力内容をデータベースへ登録する。主キー項目は日付+時刻とする。
            主キーが重複するデータがデータベース内にある場合、後から入力されたデータは登録しない。
      ３）出力仕様
          ・呼び出し元へ返却するオブジェクトはDataFrame形式で返却する。
          ・ファイルへ保存する場合はcsv形式で保存する。
            先頭１行目はヘッダレコードで、２行目以降はデータレコードとする。
          ・出力条件は日付と時刻の範囲指定によって抽出する。その他要件に合わせて条件を変更しても良いが、
            用途に合わせて関数を個別に実装すること。
          ・データベースからのデータは原則生データとする。
          ・データを集計、整形する場合はDataFrameで実行する。
    呼び出し元モジュールからの用法：
      １）当モジュールをインポートする
      ２）まだsqliteファイルが未作成の場合はcreateDb()を実行する。
      ３）importCsvを実行する。引数にはScraper.pyで出力したcsvファイル名を指定する。
      以上。
    メインモジュールとしての用法：
    　python Dao.py csv名
    　
'''
import sys, os
import logging
import pandas as pd
import numpy as np
from sqlite3 import Connection, Cursor
from sqlite3 import DatabaseError

__mylogger = logging.getLogger('WeatherDaoLogger')
dbFilePath = 'WeatherInfo.sqlite'

def __isExistsDb():
    if os.path.exists(dbFilePath) == False:
        return False
    
    db = None
    try:
        db = Connection(dbFilePath)
        return True
    except Exception as ex:
        return False
    finally:
        if db != None:
            db.close()
    
def createDb():
    if __isExistsDb() == False:
        db = Connection(dbFilePath)
        statement = "CREATE TABLE WeatherInfo ("
        statement = statement + " prec_no INTEGER,"
        statement = statement + " block_no INTEGER,"
        statement = statement + " date TEXT,"
        statement = statement + " hour INTEGER,"
        statement = statement + " pressure_onland REAL DEFAULT 0.0,"
        statement = statement + " pressure_onsea REAL DEFAULT 0.0,"
        statement = statement + " amount_rain REAL DEFAULT 0.0,"
        statement = statement + " temperature REAL DEFAULT 0.0,"
        statement = statement + " dew_point_temp REAL DEFAULT 0.0,"
        statement = statement + " vapor_pressure REAL DEFAULT 0.0,"
        statement = statement + " moisture INTEGER DEFAULT 0,"
        statement = statement + " wind_speed REAL DEFAULT 0.0,"
        statement = statement + " wind_direction TEXT DEFAULT '',"
        statement = statement + " sunlight REAL DEFAULT 0.0,"
        statement = statement + " total_solar_radiation REAL DEFAULT 0.0,"
        statement = statement + " amount_snowfall REAL DEFAULT 0.0,"
        statement = statement + " amount_snowcover REAL DEFAULT 0.0,"
        statement = statement + " weather TEXT DEFAULT '',"
        statement = statement + " amount_cloud TEXT DEFAULT '',"
        statement = statement + " visibility REAL DEFAULT 0.0,"
        statement = statement + " PRIMARY KEY (prec_no, block_no, date, hour)"
        statement = statement + ")"
        db.cursor().execute(statement)
    else:
        __mylogger.error('Database already exists. nothing to do create table.')

def importCsv(csvName):
    __mylogger.info('importing start!')
    
    try:
        #csvファイル読み込み
        df = pd.read_csv(csvName, encoding='utf-8', parse_dates=True, header=0)
        
        #INTEGERの精度変更　objectにする
        #なぜかint64のままだとsqliteにBLOBとして登録される。int8やint32に変更すると文字化けしてしまう。
        #objectにすると自動判定でintと判断されるようである。
        df['県番号'] = df['県番号'].astype('object')
        df['地区番号'] = df['地区番号'].astype('object')
        df['時刻'] = df['時刻'].astype('object')
        df['湿度(%)'] = df['湿度(%)'].astype('object')
        
        '''
        #欠損値の補完
        def __conv00(val):
            if val == '--' or val == None:
                return 0.0
            else:
                return val
        df['降水量'] = df['降水量'].apply(__conv00)
        df['日照時間(h)'] = df['日照時間(h)'].fillna(0.0)
        df['全天日射量(MJ/m2)'] = df['全天日射量(MJ/m2)'].fillna(0.0)
        df['降雪(cm)'] = df['降雪(cm)'].fillna(0.0)
        df['降雪(cm)'] = df['降雪(cm)'].apply(__conv00)
        df['積雪(cm)'] = df['積雪(cm)'].fillna(0.0)
        df['積雪(cm)'] = df['積雪(cm)'].apply(__conv00)
        df['天気'] = df['天気'].fillna('')
        df['雨雲'] = df['雨雲'].fillna('')
        df['視程(km)'] = df['視程(km)'].fillna(0.0)
        '''
        
        #dbのオープン
        db = Connection(dbFilePath)
        
        #データ一括登録
        for idx in range(0, len(df)):
            __mylogger.debug("\n" + str(df.iloc[idx]))
            statement = "INSERT INTO WeatherInfo VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            try:
                db.cursor().execute(statement, df.iloc[idx])
            except DatabaseError as insertEx:
                __mylogger.error('error has occured.')
                __mylogger.error(insertEx)
        
        db.commit()
        
    except Exception as ex:
        __mylogger.error('error has occured.')
        __mylogger.error(ex)
        if db != None:
            db.rollback()
    finally:
        if db != None:
            db.close()
        __mylogger.info('importing ended')
        
def selectByDateRange(prec_no, block_no, fromDate, toDate):
    __mylogger.info('selectByDateRange start!')
    
    try:
        #dbのオープン
        db = Connection(dbFilePath)
        
        #データ取得
        statement = "SELECT * FROM WeatherInfo WHERE"
        statement = statement + " date BETWEEN '{fromDate}' and '{toDate}' ".format(fromDate=fromDate, toDate=toDate)
        result = np.array([])
        for item in db.cursor().execute(statement):
            if len(result) > 0:
                result = np.vstack([result, item])
            else:
                result = np.array(item)
            #result.append(item)
        return result
    
    except Exception as ex:
        __mylogger.error('error has occured.')
        __mylogger.error(ex)
        if db != None:
            db.rollback()
    finally:
        if db != None:
            db.close()
        __mylogger.info('selectByDateRange ended')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if len(sys.argv) > 2:
        dbFilePath = sys.argv[2]
    createDb()
    importCsv(sys.argv[1])
    
'''
if len(sys.argv) > 2:
    for item in selectByDateRange(91, 47936, sys.argv[2], sys.argv[3]):
        print(item)
'''