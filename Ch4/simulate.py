import apache_beam as beam
import csv

if __name__ == '__main__':
    with beam.Pipeline("DirectRunner") as pipeline:
        airports = (
            pipeline
            | beam.io.ReadFromText("data/airports.csv.gz")
            | beam.Map(lambda line: next(csv.reader(([line]))))
            | beam.Map(lambda fields: (fields[0], (fields[21], fields[26])))
        )

        airports | beam.Map(
            lambda line: f'{line[0]},{",".join(line[1])}'
        ) | beam.io.textio.WriteToText("data/extracted_airports")

        pipeline.run()