"""事前準備
https://console.cloud.google.com/apis/credentials?project=scalable-data-science&authuser=3&folder&hl=ja&organizationId&supportedpurview=project
で認証情報を登録後 ./env配下に置くこと
とすること
"""
#%%
import os
import logging
from urllib.request import urlopen as impl
from urllib.parse import urlencode
import zipfile
from  google.cloud import storage
import datetime
import tempfile



#%%


# 固定値
PARAMS="UserTableName=On_Time_Performance&DBShortName=&RawDataTable=T_ONTIME&sqlstr=+SELECT+FL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE+FROM++T_ONTIME+WHERE+Month+%3D{1}+AND+YEAR%3D{0}&varlist=FL_DATE%2CUNIQUE_CARRIER%2CAIRLINE_ID%2CCARRIER%2CFL_NUM%2CORIGIN_AIRPORT_ID%2CORIGIN_AIRPORT_SEQ_ID%2CORIGIN_CITY_MARKET_ID%2CORIGIN%2CDEST_AIRPORT_ID%2CDEST_AIRPORT_SEQ_ID%2CDEST_CITY_MARKET_ID%2CDEST%2CCRS_DEP_TIME%2CDEP_TIME%2CDEP_DELAY%2CTAXI_OUT%2CWHEELS_OFF%2CWHEELS_ON%2CTAXI_IN%2CCRS_ARR_TIME%2CARR_TIME%2CARR_DELAY%2CCANCELLED%2CCANCELLATION_CODE%2CDIVERTED%2CDISTANCE&grouplist=&suml=&sumRegion=&filter1=title%3D&filter2=title%3D&geo=All%A0&time=March&timename=Month&GEOGRAPHY=All&XYEAR={0}&FREQUENCY=3&VarDesc=Year&VarType=Num&VarDesc=Quarter&VarType=Num&VarDesc=Month&VarType=Num&VarDesc=DayofMonth&VarType=Num&VarDesc=DayOfWeek&VarType=Num&VarName=FL_DATE&VarDesc=FlightDate&VarType=Char&VarName=UNIQUE_CARRIER&VarDesc=UniqueCarrier&VarType=Char&VarName=AIRLINE_ID&VarDesc=AirlineID&VarType=Num&VarName=CARRIER&VarDesc=Carrier&VarType=Char&VarDesc=TailNum&VarType=Char&VarName=FL_NUM&VarDesc=FlightNum&VarType=Char&VarName=ORIGIN_AIRPORT_ID&VarDesc=OriginAirportID&VarType=Num&VarName=ORIGIN_AIRPORT_SEQ_ID&VarDesc=OriginAirportSeqID&VarType=Num&VarName=ORIGIN_CITY_MARKET_ID&VarDesc=OriginCityMarketID&VarType=Num&VarName=ORIGIN&VarDesc=Origin&VarType=Char&VarDesc=OriginCityName&VarType=Char&VarDesc=OriginState&VarType=Char&VarDesc=OriginStateFips&VarType=Char&VarDesc=OriginStateName&VarType=Char&VarDesc=OriginWac&VarType=Num&VarName=DEST_AIRPORT_ID&VarDesc=DestAirportID&VarType=Num&VarName=DEST_AIRPORT_SEQ_ID&VarDesc=DestAirportSeqID&VarType=Num&VarName=DEST_CITY_MARKET_ID&VarDesc=DestCityMarketID&VarType=Num&VarName=DEST&VarDesc=Dest&VarType=Char&VarDesc=DestCityName&VarType=Char&VarDesc=DestState&VarType=Char&VarDesc=DestStateFips&VarType=Char&VarDesc=DestStateName&VarType=Char&VarDesc=DestWac&VarType=Num&VarName=CRS_DEP_TIME&VarDesc=CRSDepTime&VarType=Char&VarName=DEP_TIME&VarDesc=DepTime&VarType=Char&VarName=DEP_DELAY&VarDesc=DepDelay&VarType=Num&VarDesc=DepDelayMinutes&VarType=Num&VarDesc=DepDel15&VarType=Num&VarDesc=DepartureDelayGroups&VarType=Num&VarDesc=DepTimeBlk&VarType=Char&VarName=TAXI_OUT&VarDesc=TaxiOut&VarType=Num&VarName=WHEELS_OFF&VarDesc=WheelsOff&VarType=Char&VarName=WHEELS_ON&VarDesc=WheelsOn&VarType=Char&VarName=TAXI_IN&VarDesc=TaxiIn&VarType=Num&VarName=CRS_ARR_TIME&VarDesc=CRSArrTime&VarType=Char&VarName=ARR_TIME&VarDesc=ArrTime&VarType=Char&VarName=ARR_DELAY&VarDesc=ArrDelay&VarType=Num&VarDesc=ArrDelayMinutes&VarType=Num&VarDesc=ArrDel15&VarType=Num&VarDesc=ArrivalDelayGroups&VarType=Num&VarDesc=ArrTimeBlk&VarType=Char&VarName=CANCELLED&VarDesc=Cancelled&VarType=Num&VarName=CANCELLATION_CODE&VarDesc=CancellationCode&VarType=Char&VarName=DIVERTED&VarDesc=Diverted&VarType=Num&VarDesc=CRSElapsedTime&VarType=Num&VarDesc=ActualElapsedTime&VarType=Num&VarDesc=AirTime&VarType=Num&VarDesc=Flights&VarType=Num&VarName=DISTANCE&VarDesc=Distance&VarType=Num&VarDesc=DistanceGroup&VarType=Num&VarDesc=CarrierDelay&VarType=Num&VarDesc=WeatherDelay&VarType=Num&VarDesc=NASDelay&VarType=Num&VarDesc=SecurityDelay&VarType=Num&VarDesc=LateAircraftDelay&VarType=Num&VarDesc=FirstDepTime&VarType=Char&VarDesc=TotalAddGTime&VarType=Num&VarDesc=LongestAddGTime&VarType=Num&VarDesc=DivAirportLandings&VarType=Num&VarDesc=DivReachedDest&VarType=Num&VarDesc=DivActualElapsedTime&VarType=Num&VarDesc=DivArrDelay&VarType=Num&VarDesc=DivDistance&VarType=Num&VarDesc=Div1Airport&VarType=Char&VarDesc=Div1AirportID&VarType=Num&VarDesc=Div1AirportSeqID&VarType=Num&VarDesc=Div1WheelsOn&VarType=Char&VarDesc=Div1TotalGTime&VarType=Num&VarDesc=Div1LongestGTime&VarType=Num&VarDesc=Div1WheelsOff&VarType=Char&VarDesc=Div1TailNum&VarType=Char&VarDesc=Div2Airport&VarType=Char&VarDesc=Div2AirportID&VarType=Num&VarDesc=Div2AirportSeqID&VarType=Num&VarDesc=Div2WheelsOn&VarType=Char&VarDesc=Div2TotalGTime&VarType=Num&VarDesc=Div2LongestGTime&VarType=Num&VarDesc=Div2WheelsOff&VarType=Char&VarDesc=Div2TailNum&VarType=Char&VarDesc=Div3Airport&VarType=Char&VarDesc=Div3AirportID&VarType=Num&VarDesc=Div3AirportSeqID&VarType=Num&VarDesc=Div3WheelsOn&VarType=Char&VarDesc=Div3TotalGTime&VarType=Num&VarDesc=Div3LongestGTime&VarType=Num&VarDesc=Div3WheelsOff&VarType=Char&VarDesc=Div3TailNum&VarType=Char&VarDesc=Div4Airport&VarType=Char&VarDesc=Div4AirportID&VarType=Num&VarDesc=Div4AirportSeqID&VarType=Num&VarDesc=Div4WheelsOn&VarType=Char&VarDesc=Div4TotalGTime&VarType=Num&VarDesc=Div4LongestGTime&VarType=Num&VarDesc=Div4WheelsOff&VarType=Char&VarDesc=Div4TailNum&VarType=Char&VarDesc=Div5Airport&VarType=Char&VarDesc=Div5AirportID&VarType=Num&VarDesc=Div5AirportSeqID&VarType=Num&VarDesc=Div5WheelsOn&VarType=Char&VarDesc=Div5TotalGTime&VarType=Num&VarDesc=Div5LongestGTime&VarType=Num&VarDesc=Div5WheelsOff&VarType=Char&VarDesc=Div5TailNum&VarType=Char"
URL='https://www.transtats.bts.gov/DownLoad_Table.asp?Table_ID=236&Has_Group=3&Is_Zipped=0'
EXPECTED_HEADER = 'FL_DATE,UNIQUE_CARRIER,AIRLINE_ID,CARRIER,FL_NUM,ORIGIN_AIRPORT_ID,ORIGIN_AIRPORT_SEQ_ID,ORIGIN_CITY_MARKET_ID,ORIGIN,DEST_AIRPORT_ID,DEST_AIRPORT_SEQ_ID,DEST_CITY_MARKET_ID,DEST,CRS_DEP_TIME,DEP_TIME,DEP_DELAY,TAXI_OUT,WHEELS_OFF,WHEELS_ON,TAXI_IN,CRS_ARR_TIME,ARR_TIME,ARR_DELAY,CANCELLED,CANCELLATION_CODE,DIVERTED,DISTANCE'
BUCKET_NAME = 'flight-records-sds-hase'
BASE_BLOB_NAME = 'raw/'






#%%
def download(YEAR ,MONTH, destdir):
    logging.info(f'requesting {YEAR}-{MONTH}')
    PARAMS_=PARAMS.format(YEAR, MONTH)
    filename = os.path.join(destdir, f'{YEAR}{MONTH}.zip')
    
    with open(filename, 'wb') as fp:
        with impl(URL, PARAMS_.encode('utf-8')) as res:
            logging.info(f'status={res.status}')
            fp.write(res.read())
        
    return filename
        
        
    


#%%
def zip_to_csv(filename, destdir):
    zip_ref = zipfile.ZipFile(filename, 'r')
    cwd = os.getcwd() # 現在のdirを取得
    os.chdir(destdir) # 保存したいdirに移動
    zip_ref.extractall()
    os.chdir(cwd) # 元の場所に戻る
    
    csvfile = os.path.join(destdir, zip_ref.namelist()[0])
    zip_ref.close()
    logging.info(f'Extracted {csvfile}')
    
    return csvfile


#%%

class DataUnavailable(Exception):
    def __init__(self, message):
        self.message = message

class UnexpectedFormat(Exception):
    def __init__(self, message):
        self.message = message


#%%

def verify_ingest(csvfile):
    with open(csvfile, 'r') as csvfp:
        firstline = csvfp.readline().strip()
        if firstline != EXPECTED_HEADER:
            os.remove(csvfile)
            msg = f'Got header={firstline}, but expected={EXPECTED_HEADER}'
            logging.error(msg)    
            raise UnexpectedFormat(msg)
    
        if next(csvfp, None) == None:
            os.remove(csvfile)
            msg = ('Received a file from BTS that has only the header and no content')
            
            raise DataUnavailable(msg)
    


#%%
def remove_quote(text):
    return text.translate(str.maketrans('','', '"')) # double quoteのみ取り除く

def remove_quotes_comma(csvfile, year, month):
    try:
        outfile = os.path.join(os.path.dirname(csvfile), f'{year}{month}.csv')
        
        with open(csvfile, 'r') as infp:
            with open(outfile, 'w') as outfp:
                for line in infp:
                    outline = line.rstrip().rstrip(',')
                    outline = remove_quote(outline)
                    outfp.write(outline)
                    outfp.write('\n')
        return outfile
        
    finally:
        # os.remove(csvfile)
        pass


#%%
def upload(csvfile, bucktname,blobname):
    client = storage.Client()
    bucket = client.get_bucket(bucktname)
    gslocation = f'{bucktname}/{blobname}'
    blob = storage.Blob(blobname, bucket)
    blob.upload_from_filename(csvfile)

    logging.info(f'Uploaded to {gslocation}')

    return gslocation


#%%
def ingest(year, month, bucketname):
    with tempfile.TemporaryDirectory() as tmpdir:
        zipfile = download(year,month, tmpdir)
        bts_csv = zip_to_csv(zipfile, tmpdir)
        csvfile = remove_quotes_comma(bts_csv, year, month)
        gslocation = os.path.join(BASE_BLOB_NAME, os.path.basename(csvfile))
        upload(csvfile, bucketname, gslocation)

        return  gslocation

#%%
def next_month(bucketname):
    client = storage.Client()
    bucket = client.get_bucket(bucketname)
    blobs = list(bucket.list_blobs(prefix = BASE_BLOB_NAME))
    files = [blob.name for blob in blobs if 'csv' in blob.name]
    lastfile = os.path.basename(files[-1])

    year = lastfile[:4]
    month = lastfile[4:6]

    return compute_next_month(year, month)

def compute_next_month(year, month):
    dt = datetime.datetime(int(year), int(month), 15)
    dt = dt+datetime.timedelta(30) # go to next month

    return f'{dt.year}', f'{dt.month:02}'

#%%
compute_next_month(2019,10)



#%%
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='BTSウェブサイトから flight record を取得し、指定したGCS上に保存します')
    parser.add_argument('--bucket', required=True)
    parser.add_argument('--year' )
    parser.add_argument('--month')

    try:
        logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
        args = parser.parse_args()
        if args.year is None or args.month is None:
            year, month = next_month(args.bucket)
        else:
            year = args.year
            month = args.month

        gcsfile = ingest(year, month, args.bucket)
        logging.info(f'Success ... ingedted at {gcsfile}')
    
    except DataUnavailable as e:
        logging.info(f'Try againg later {e.message}')



#%%
