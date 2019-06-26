#%%
import os
import logging
import ingest_flight
import flask


app = flask.Flask(__name__)

CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)


@app.route('/')
def welcome():
    return  '<html><a href="ingest">ingest next month</a> flight data</html>'


@app.route('/ingest')
def ingest_next_month():
    status = ''
    try:
        is_cron = flask.request.headers['X-Appengine-Cron']
        logging.info(f'Received cron request {is_cron}')

        bucket = CLOUD_STORAGE_BUCKET
        year, month = ingest_flight.next_month(bucket)
        status = f'scheduling ingest of year={year}, month={month}'
        logging.info(status)

        gcfile = ingest_flight.ingest(year, month, bucket)
        status = f'successfly ingesetd={gcfile}'
        logging.info(status)
    
    except ingest_flight.DataUnavailable:
        status = f'File for {year}-{month} not available yet'
        logging.info(status)

    
    except KeyError:
        status = f'this capability is accessible only by Cron service'
        logging.info('Rejected non-Cron request')

    return status

@app.errorhandler(500)
def server_error(e):
    logging.exception('An error occoured during a request.')

    return f"""
    An internal error occurred: <pre>{e}</pre>
    See logs for full stacktrace.
    """, 500

if __name__ =='__main__':
    app.run(host='127.0.0.1', port=8000, debug=True)

