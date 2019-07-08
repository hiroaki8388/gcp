import apache_beam as beam
import csv

logger = logging.getLogger(__name__)
#%%
def addtimezone(lat, lon):
    try:
        import timezonefinder

        tf = timezonefinder.TimezoneFinder()
        tz = tf.timezone_at(lng=float(lon), lat=float(lat))

        if tz is None:
            tz = "UTC"
        return (lat, lon, tz)
    except ValueError:
        return (lat, lon, "TIMEZONE")

def as_utc(date, hhmm, tzone):
    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime, pytz
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(datetime.datetime.strptime(date, '%Y-%m-%d'), is_dst=False)
            # 日付に出発/到着時刻を追加
            loc_dt += datetime.timedelta(hours=int(hhmm[:2]), minutes=hhmm[:2], is_dst=False)
            utc_dt = loc_dt.astimezone(pytz.utc)

            # (utcに変換した出発/到着時間, utcとlocalの時間の差分) 
            return utc_dt.strftime('%Y-%m-%d %H:%M:%S'), loc_dt.utcoffset().total_seconds()

        else:
            return '', 0
    
    except ValueError as e:
        logger.error(TODO)


#%%
# TODO あとで消す
import  pytz
import  datetime

tz = pytz.timezone('Asia/Tokyo')
day=tz.localize(datetime.datetime(2019, 5, 10),is_dst=False)
day.utcoffset().total_seconds()/3600

#%%
datetime.datetime(2010,10,10).total_seconds()
#%%
if __name__ == "__main__":
    with beam.Pipeline("DirectRunner") as pipeline:
        airports = (
                pipeline
            | beam.io.ReadFromText("data/airports.csv.gz")
            | beam.Map(lambda line: next(csv.reader(([line]))))
            | beam.Map(lambda fields: (fields[0], addtimezone(fields[21], fields[26])))
        )

        airports | beam.Map(
            lambda line: f'{line[0]},{",".join(line[1])}'
        ) | beam.io.textio.WriteToText("data/extracted_airports")

        flights = pipeline | "flights:read" >> beam.io.ReadFromText(
            "data/201606_part.csv")
            | 'flights: tzcorr'  >> beam.FlatMap(tz_)

    pipeline.run()


#%%
