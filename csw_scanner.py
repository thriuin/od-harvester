import argparse
import dateutil.parser
import logging

from ConfigParser import ConfigParser
from db_schema import ECRecord, add_record, connect_to_database, find_record_by_uuid, get_setting, save_setting
from owslib.csw import CatalogueServiceWeb
from owslib.fes import PropertyIsGreaterThanOrEqualTo
from owslib.namespaces import Namespaces
from colorama import init, Fore, Style
from datetime import datetime

# Init colorama
init(autoreset=True)

argparser = argparse.ArgumentParser(
    description="Scan Environment Canada's CSW server and save record(s) to the Open Data harvester database"
)
argparser.add_argument('-s', '--since', action='store', default='', dest='since',
                       help='Scan since date in ISO 8601 format (e.g. 2015-02-11T00:00:00)')
argparser.add_argument('-l', '--log', action='store', default='', dest='log_filename', help='Log file')
argparser.add_argument('-m', '--monitor', action='store_true', default=False, dest='monitor',
                       help='Use the last scan date which was saved the last time the scanner was run')
argparser.add_argument('-a', '--all', action='store_true', default=False, dest='all')

args = argparser.parse_args()

if args.log_filename != '':
    logging.basicConfig(filename=args.log_filename, level=logging.WARNING,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m/%d/%Y %I:%M:%S %p')


class CswScanner:

    def __init__(self):
        self.napids = []
        self.start_pos = 0

        # Get the CSW URL, Username and Password

        ini_config = ConfigParser()
        ini_config.read('harvester.ini')
        csw_url = ini_config.get('csw', 'csw.url')
        csw_user = ini_config.get('csw', 'csw.username')
        csw_passwd = ini_config.get('csw', 'csw.password')

        self.csw = CatalogueServiceWeb(csw_url, username=csw_user, password=csw_passwd, timeout=20)

    def get_all_ids(self, since=None):

        while True:
            if since is not None:
                scan_date = since.strftime('%Y-%m-%d')
                since_query = PropertyIsGreaterThanOrEqualTo('Modified', scan_date)
                self.csw.getrecords2(esn='brief', startposition=self.start_pos, constraints=[since_query])
            else:
                self.csw.getrecords2(esn='brief', startposition=self.start_pos)
            if self.csw.results['returned'] == 0:
                break
            print '{0}Found {1}{2}{3} records'.format(Fore.GREEN, Fore.BLUE, self.csw.results['matches'], Fore.GREEN)
            print '{0}Next record: {1}{2}'.format(Fore.GREEN, Fore.BLUE, self.csw.results['nextrecord'])
            self.start_pos = self.csw.results['nextrecord']

            for rec in self.csw.records:
                try:
                    print u'{0}{1}{2}: {3}'.format(Fore.RED, rec, Fore.CYAN, self.csw.records[rec].title.decode('utf-8'))
                except UnicodeEncodeError:
                    print u'{0}Unprintable title for {1}{2}'.format(Fore.GREEN, Fore.RED, rec)
                self.napids.append(rec)

    def load_naps(self):

        ns = Namespaces()
        gmd = ns.get_namespace('gmd')
        session = connect_to_database()

        for napid in self.napids:

            print '{0}Full NAP Record for {1}{2}'.format(Fore.GREEN, Fore.CYAN, napid)
            self.csw.getrecordbyid(id=[napid], outputschema=gmd)

            ec_rec = find_record_by_uuid(session, napid, query_class=ECRecord)

            if ec_rec is None:
                ec_rec = ECRecord(
                    uuid=self.csw.records[napid].identifier,
                    title=self.csw.records[napid].identification.title,
                    state='active',
                    nap_record=self.csw.records[napid].xml,
                    csw_scanned=datetime.now().isoformat()
                )
            else:
                ec_rec.title = self.csw.records[napid].identification.title,
                ec_rec.state = 'active',
                ec_rec.nap_record = self.csw.records[napid].xml,
                ec_rec.csw_scanned = datetime.now().isoformat()

            add_record(session, ec_rec)

        session.close_all()

# Temporary main

eccsw = CswScanner()
scan8601 = None
scan_date = None

if args.all:
    scan_date = None
elif args.monitor:
    monitor_date = get_setting('csw_last_scan_date')
    scan_date = datetime.now()
    if monitor_date.setting_value is not None:
        scan_date = dateutil.parser.parse(monitor_date.setting_value)
elif args.since != '':
    scan_date = dateutil.parser.parse(args.since)
    if scan_date is None:
        logging.error('Invalid date: ' + args.since)
        exit()

eccsw.get_all_ids(scan_date)
eccsw.load_naps()

monitor_date = get_setting('csw_last_scan_date')
scan_date = datetime.now()
monitor_date.setting_value = scan_date.isoformat()
save_setting(monitor_date)
