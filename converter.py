__author__ = 'Statistics Canada'
__license__ = 'MIT'

from datetime import datetime
from db_schema import connect_to_database, find_all_records, add_record, Packages, find_record_by_uuid, \
                      Settings, get_setting, save_setting, GeogratisRecord, ECRecord
from ec_dataset_factory import MetadataDatasetModelECFactory
from geogratis_dataset_factory import MetadataDatasetModelGeogratisFactory
from ConfigParser import ConfigParser
import argparse
import json
import logging
import time
import traceback


argparser = argparse.ArgumentParser(
    description='Convert Geogratis records to the Canada Open Data schema'
)
argparser.add_argument('-s', '--since', action='store', default='', dest='since',
                       help='Only update Geogratis records scanned since date (e.g. 2014-01-21)')
argparser.add_argument('-m', '--monitor', action='store_true', dest='monitoring',
                       help='Only convert Geogratis records scanned since the last run of the converter')
argparser.add_argument('-t', '--type', action='store', dest='scan_type', default='gr',
                       help='Type of harvest data to convert: e.g. ec or gr')

def main(since, scan_type):

    now_str = datetime.now().strftime('%Y-%m-%dT%H:%M:%S.000Z')
    if scan_type == 'gr':
        factory = MetadataDatasetModelGeogratisFactory()
        setting = get_setting('last_conversion_gr')
        query_class = GeogratisRecord
        if setting is None:
            setting = Settings()
            setting.setting_name = 'last_conversion_gr'
    else:
        factory = MetadataDatasetModelECFactory()
        setting = get_setting('last_conversion_ec')
        query_class = ECRecord
        if setting is None:
            setting = Settings()
            setting.setting_name = 'last_conversion_ec'

    # Potentially doing a VERY large ORM query. If we don't limit the read, then SQLAlchemy will try to pull
    # everything into memory. Therefore the query must be paged. Paging requires keeping track of the sequential
    # record ID's

    session = connect_to_database()
    last_id = 0
    scan_date = None

    if since != '':
        try:
            scan_date = datetime.fromtimestamp(time.mktime(time.strptime(args.since, '%Y-%m-%d')))
        except ValueError:
            logging.error("Incorrect since date format. Use YYYY-MM-DD")
            session.close()
            exit()
        except Exception, e:
            logging.error(e.message)
            session.close()
            exit()
    elif args.monitoring:
        if setting.setting_value is not None:
            scan_date = datetime.strptime(setting.setting_value, '%Y-%m-%dT%H:%M:%S.000Z')

    setting.setting_value = now_str

    while True:
        scan_records = find_all_records(session, query_limit=10, limit_id=last_id, cutoff=scan_date,
                                        query_class=query_class)

        if len(scan_records) == 0:
            break
        else:
            for scan_record in scan_records:
                try:
                    if scan_date and scan_record.scanned:
                        if scan_record.geogratis_scanned < scan_date:
                            last_id = scan_record.id
                            continue
                    print 'ID: {0}'.format(scan_record.id)
                    # In order to avoid multiple updates, only allow for one instance of an update per uuid.
                    # Previous updates are overridden with the latest update
                    pkg_update_record = find_record_by_uuid(session, scan_record.uuid, query_class=Packages)
                    if pkg_update_record is None:
                        pkg_update_record = Packages()
                    if scan_record.state == 'active':
                        # Retrieve the entire Geogratis record in CKAN format and then populate the fields of
                        # the Package Update record for the database.
                        geo_record = factory.create_model(scan_record.uuid)
                        pkg_update_record.uuid = scan_record.uuid

                        if geo_record is not None:
                            # Set the dataset for immediate release on the Registry
                            geo_record.portal_release_date = time.strftime("%Y-%m-%d")
                            geo_record.ready_to_publish = True

                            pkg_update_record.ckan_json = json.dumps(geo_record.as_dict())

                            current_time_str = time.strftime("%Y-%m-%d %H:%M:%S")
                            if not pkg_update_record.created:
                                pkg_update_record.created = current_time_str
                            pkg_update_record.updated = current_time_str
                            pkg_update_record.source = scan_type
                            add_record(session, pkg_update_record)
                except Exception, e:
                    logging.error(e.message)
                    traceback.print_exc()
                last_id = scan_record.id
    save_setting(setting)
    session.close()

args = argparser.parse_args()
main(since=args.since, scan_type=args.scan_type)