__author__ = 'Statistics Canada'
__license__ = 'MIT'

import argparse
import logging
import requests
import simplejson as json
from colorama import init, Fore, Style
from datetime import datetime
from db_schema import connect_to_database, GeogratisRecord, add_record, find_record_by_uuid, get_setting, save_setting
from time import sleep

# Init colorama
init(autoreset=True)

# Set up command line arguments

argparser = argparse.ArgumentParser(
    description='Scan Geogratis and save record to a database'
)
argparser.add_argument('-d', '--since', action='store', default='', dest='since',
                       help='Scan since date (e.g. 2014-01-21)')
argparser.add_argument('-l', '--log', action='store', default='', dest='log_filename', help='Log to file')
argparser.add_argument('-s', '--start_index', action='store', default='', dest='start_index',
                       help='Start-index')
argparser.add_argument('-m', '--monitor', action='store_true', default=False, dest='monitor')

args = argparser.parse_args()

if args.log_filename != '':
    logging.basicConfig(filename=args.log_filename, level=logging.WARNING,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')

def _get_link(geo_page, link_rel='next'):
    next_link = ''
    for link in geo_page['links']:
        if link['rel'] == link_rel:
            next_link = link['href']
            logging.warn(next_link)
            break
    return next_link


def get_geogratis_rec(uuid, lang='en', data_format='json'):
    geog_url = 'http://geogratis.gc.ca/api/{0}/nrcan-rncan/ess-sst/{1}.{2}'.format(
        lang, uuid, data_format)
    r = requests.get(geog_url)
    if r.status_code == 200 and data_format == 'json':
        geo_result = r.json()
    else:
        logging.error('HTTP Error: {0}'.format(r.status_code))
        geo_result = None
    sleep(0.3)
    return geo_result


def main(since='', start_index='', monitor=False):
    geog_url = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?alt=json&max-results=100'
    monitor_setting = get_setting('monitor_link')
    if monitor:
        if monitor_setting.setting_value is None:
            geog_url = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?edited-min=2015-01-01&alt=json&max-results=100'
        else:
            geog_url = monitor_setting.setting_value
    elif since != '':
        geog_url = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst?edited-min={0}&alt=json&max-results=100'.format(since)
    elif start_index != '':
        geog_url = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst/?start-index={0}&alt=json&max-results=100'.format(start_index)
    print ('{0}Scanning: {1}{2}'.format(Fore.GREEN, Fore.BLUE, geog_url))
    r = requests.get(geog_url)
    logging.info('HTTP Response Status {0}'.format(r.status_code))
    session = None
    try:
        session = connect_to_database()
        # Get the first page of the feed
        if r.status_code == 200:
            feed_page = r.json()

            # Save the monitor link for future use
            monitor_link = _get_link(feed_page, 'monitor')
            if monitor_link != '':

                monitor_setting.setting_value = monitor_link
                save_setting(monitor_setting)
                print  "{0}Next Monitor Link: {1}{2}".format(Fore.YELLOW, Fore.BLUE, monitor_setting.setting_value)
            next_link = _get_link(feed_page)

            print ('{0}{1} Records Found'.format(Fore.BLUE, feed_page['count']))

            if 'products' in feed_page:
                for product in feed_page['products']:
                    try:
                        save_geogratis_record(session, product['id'])
                    except Exception, e:
                        logging.error('{0} failed to load'.format(product['id']))
                        logging.error(e)

            # Keep polling until exhausted
            while next_link != '':
                geog_url = next_link
                r = requests.get(geog_url)
                feed_page = r.json()
                next_link = _get_link(feed_page)
                print '{0}Next page link: {1}{2}'.format(Fore.YELLOW, Fore.BLUE, next_link)
                if 'products' in feed_page:
                    for product in feed_page['products']:

                        # Don't crash on every call - log the error and continue
                        try:
                            save_geogratis_record(session, product['id'])
                        except Exception, e:
                            logging.error('{0} failed to load'.format(product['id']))
                            logging.error(e)
                save_setting(monitor_setting)

    except Exception, e:
        logging.error(e)
    finally:
        if session is not None:
            session.close_all()


def save_geogratis_record(session, uuid):
    msg = 'Retrieving data set {0}'.format(uuid)
    logging.info(msg)
    print(msg)
    geo_rec_en = get_geogratis_rec(uuid)
    geo_rec_fr = get_geogratis_rec(uuid, 'fr')
    if not geo_rec_en is None:
        state = 'deleted'
        title_fr = ''
        if geo_rec_en['deleted'] == 'false':
            state = 'active'
        if geo_rec_fr is None:
            state = 'missing french'
        else:
            title_fr = geo_rec_fr['title']
        new_rec = find_record_by_uuid(session, geo_rec_en['id'])

        created_date = '2000-01-01'
        updated_date = '2000-01-01'
        edited_date = '2000-01-01'
        geogratis_scanned = datetime.now().isoformat()
        if state != 'deleted':
            created_date = geo_rec_en['publishedDate']
            updated_date = geo_rec_en['updatedDate']
            edited_date = geo_rec_en['editedDate']

        if new_rec is None:
            new_rec = GeogratisRecord(uuid=geo_rec_en['id'],
                                      title_en=geo_rec_en['title'],
                                      title_fr=title_fr,
                                      json_record_en=json.dumps(geo_rec_en),
                                      json_record_fr=json.dumps(geo_rec_fr),
                                      created=created_date,
                                      updated=updated_date,
                                      edited=edited_date,
                                      state=state,
                                      geogratis_scanned=geogratis_scanned)
        else:
            new_rec.title_en = geo_rec_en['title']
            new_rec.title_fr = title_fr
            new_rec.json_record_en = json.dumps(geo_rec_en)
            new_rec.json_record_fr = json.dumps(geo_rec_fr),
            new_rec.created = created_date,
            new_rec.updated = updated_date,
            new_rec.edited = edited_date,
            new_rec.state = state
            new_rec.geogratis_scanned = geogratis_scanned

        add_record(session, new_rec)

# Run the scanner

if args.monitor:
    main('', '', True)
elif args.since != '':
    main(since=args.since)
elif args.start_index != '':
    main(start_index=args.start_index)
else:
    main()
print 'Scan completed {0}{1}'.format(Style.BRIGHT, datetime.now().isoformat())
