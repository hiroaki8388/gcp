import apache_beam as beam
import csv


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
        if (hhmm) > 0 and tzone is not None:
            import datetime, pytz
            

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
            "data/201605_part.csv")
            | 'flights: tzcorr'  >> beam.FlatMap(tz_)

        pipeline.run()
