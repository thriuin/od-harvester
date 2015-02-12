__author__ = 'Statistics Canada'
__copyright__ = 'Crown Copyright'
__license__ = 'MIT'

import ckanapi
import logging
import re
import simplejson as json

from ConfigParser import ConfigParser
from db_schema import connect_to_database, find_record_by_uuid
from metadata_model import MetadataDatasetModel, MetadataResourcesModel


class MetadataDatasetModelGeogratisFactory():

    od_regions = {}
    od_topics = {}
    od_topic_subjects = {}
    od_subjects = {}
    od_resource_formats = {}
    od_presentation_forms = {}

    def __init__(self):

        # Read from the schema.json file to get a list of acceptable choices for the fields in the Open Data schema
        od_schema_file = open('schema.json', 'r')
        schema_model = json.load(od_schema_file)
        dataset_fields = []
        for s in schema_model['dataset_sections']:
            dataset_fields.extend(s['fields'])
        dataset_field_by_id = dict((f['id'], f) for f in dataset_fields)
        resource_fields = schema_model['resource_fields']
        resource_field_by_id = dict((f['id'], f) for f in resource_fields)

        for r in dataset_field_by_id['geographic_region']['choices']:
            self.od_regions[r['eng']] = r['key']

        for t in dataset_field_by_id['topic_category']['choices']:
            self.od_topics[t['eng']] = t['key']

        # For convenience, let's have a dictionary of subjects by subject key
        subjects = {}
        for b in dataset_field_by_id['topic_category']['choices']:
            subjects[b['id']] = b

        for t in dataset_field_by_id['topic_category']['choices']:
            self.od_topic_subjects[t['eng']] = []
            for s in t['subject_ids']:
                self.od_topic_subjects[t['eng']].append(s)

        for f in dataset_field_by_id['subject']['choices']:
            self.od_subjects[f['id']] = f['key']
        for r in resource_field_by_id['format']['choices']:
            self.od_resource_formats[r['eng']] = r['key']
        # Additional mappings to the correct types are added because the file formats in Geogratis
        # do not match one-for-one with Open Data formats
        self.od_resource_formats['GeoTIFF (Georeferenced Tag Image File Format)'] = 'geotif'
        self.od_resource_formats['TIFF (Tag Image File Format)'] = "tiff"
        self.od_resource_formats['GeoTIFF'] = 'geotif'
        self.od_resource_formats['Adobe PDF'] = 'PDF'
        self.od_resource_formats['PDF - Portable Document Format'] = "PDF"
        self.od_resource_formats['ASCII (American Standard Code for Information Interchange)'] = "TXT"
        self.od_resource_formats['GML (Geography Markup Language)'] = "gml"
        self.od_resource_formats['Shape'] = "SHAPE"
        self.od_resource_formats['gzip (GNU zip)'] = "ZIP"
        self.od_resource_formats['ZIP'] = "ZIP"
        self.od_resource_formats['ESRI Shapefile'] = "SHAPE"
        self.od_resource_formats['JPEG'] = "jpg"
        self.od_resource_formats['Jpeg 2000'] = "jpeg 2000"

        self.od_presentation_forms['documentDigital'] = u"Document Digital | Document num\u00e9rique"
        self.od_presentation_forms['documentHardcopy'] = u"Document Hardcopy | Document papier"
        self.od_presentation_forms['imageDigital'] = u"Image Digital | Image num\u00e9rique"
        self.od_presentation_forms['imageHardcopy'] = u"Image Hardcopy | Image papier"
        self.od_presentation_forms['mapDigital'] = u"Map Digital | Carte num\u00e9rique"
        self.od_presentation_forms['mapHardcopy'] = u"Map Hardcopy | Carte papier"
        self.od_presentation_forms['modelDigital'] = u"Model Digital | Mod\u00e8le num\u00e9rique"
        self.od_presentation_forms['modelHardcopy'] = u"Model Hardcopy | Maquette"
        self.od_presentation_forms['profileDigital'] = u"Profile Digital | Profil num\u00e9rique"
        self.od_presentation_forms['profileHardcopy'] = u"Profile Hardcopy | Profil papier"
        self.od_presentation_forms['tableDigital'] = u"Table Digital | Table num\u00e9rique"
        self.od_presentation_forms['tableHardcopy'] = u"Table Hardcopy | Table papier"
        self.od_presentation_forms['videoDigital'] = u"Video Digital | Vid\u00e9o num\u00e9rique"
        self.od_presentation_forms['videalHardcopy'] = u"Video Hardcopy | Vid\u00e9o film"
        self.od_presentation_forms['audioDigital'] = u"Audio Digital | Audio num\u00e9rique"
        self.od_presentation_forms['audioHardcopy'] = u"Audio Hardcopy | Audio analogique"
        self.od_presentation_forms['multimediaDigital'] = u"Multimedia Digital | Multim\u00e9dia num\u00e9rique"
        self.od_presentation_forms['multimediaHardcopy'] = u"Multimedia Hardcopy | Multim\u00e9dia analogique"
        self.od_presentation_forms['diagramDigial'] = u"Diagram Digital | Diagramme num\u00e9rique"
        self.od_presentation_forms['diagramHardcopy'] = u"Diagram Hardcopy | Diagramme papier"


    def create_model(self, uuid):
        session = connect_to_database()
        try:
            geogratis_rec = find_record_by_uuid(session, uuid)
            geo_rec_en = json.loads(geogratis_rec.json_record_en)
            geo_rec_fr = json.loads(geogratis_rec.json_record_fr)
        finally:
            session.close()

        # Even if the French or English record is missing, create an object with

        return self.convert_geogratis_json(geo_rec_en, geo_rec_fr)


    def convert_geogratis_json(self, geo_obj_en, geo_obj_fr):

        ds = MetadataDatasetModel()
        ds.owner_org = 'nrcan-rncan'
        ds.catalog_type = u'Geo Data | G\u00e9o'

        if not geo_obj_en is None:
            ds.id = geo_obj_en['id']
            ds.url = 'http://geogratis.gc.ca/api/en/nrcan-rncan/ess-sst/{0}'.format(geo_obj_en['id'])
            ds.title = geo_obj_en['title']
            ds.notes = geo_obj_en['summary']
            ds.date_modified = geo_obj_en.get('updatedDate', '2000-01-01')
            if 'citation' in geo_obj_en:
                ds.data_series_name = geo_obj_en['citation']['series']

            # Keywords are extracted from two sources in the Geogratis record: the gc:subject category
            # and the keywords list
            en_keywords = []
            en_subjects = self._get_category(geo_obj_en['categories'], 'urn:gc:subject')
            for term in en_subjects:
                en_keywords.append(self._clean_keyword(term['label']))
            ds.keywords = self._extract_keywords(geo_obj_en.get('keywords', []), en_keywords)

            # CKAN needs to treat the GeoJSON as a string to store, not as actual JSON
            ds.spatial = unicode(str(geo_obj_en['geometry']).replace("u'", '\"').replace("'", '\"'))

            if ('citation' in geo_obj_en) and ('presentationForm' in geo_obj_en['citation']):
                for form in geo_obj_en['citation']['presentationForm'].split():
                    if form.strip(';') in self.od_presentation_forms:
                        ds.presentation_form = self.od_presentation_forms[form.strip(';')]

            if 'browseImages' in geo_obj_en and len(geo_obj_en['browseImages']) > 0:
                ds.browse_graphic_url = geo_obj_en['browseImages'][0]['link']
            else:
                ds.browse_graphic_url = "/static/img/canada_default.png"
            if ('citation' in geo_obj_en) and ('otherCitationDetails' in geo_obj_en['citation']):
                ds.digital_object_identifier = geo_obj_en['citation']['otherCitationDetails']

            # Must match Geogratis place names against those used by Open Data
            terms = self._get_category(geo_obj_en['categories'], 'urn:iso:place')
            for term in terms:
                if term['label'] in self.od_regions:
                    ds.geographic_region.append(self.od_regions[term['label']])

            if ('citation' in geo_obj_en) and ('seriesIssue' in geo_obj_en['citation']):
                ds.data_series_issue_identification = geo_obj_en['citation']['seriesIssue']

            if ('publishedDate' in geo_obj_en):
                ds.date_published = geo_obj_en['publishedDate']
            elif ('citation' in geo_obj_en) and ('publicationDate' in geo_obj_en['citation']) and \
                    0 < len(geo_obj_en['citation']['publicationDate']):
                ds.date_published = geo_obj_en['citation']['publicationDate']
            elif 'createdDate' in geo_obj_en:
                ds.date_published = geo_obj_en['createdDate']
            if len(ds.date_published) == 7:
                ds.date_published = '%s-01' % ds.date_published
            if len(ds.date_published) == 4:
                ds.date_published = '%s-01-01' % ds.date_published

            if 'topicCategories' in geo_obj_en:
                for topic in geo_obj_en['topicCategories']:
                    # Test for a non-standard exceptions specific to Geogratis
                    if topic == u'society; soci\u00e9t\u00e9':
                        topic = "society"
                    elif topic == "farming; agriculture":
                        topic = "farming"
                    # Transform topic from Geogratis format to Open Data format.
                    # e.g. geoscientificInformation -> Geoscientific Information
                    topic_key = re.sub("([a-z])([A-Z])", "\g<1> \g<2>", topic).title()

                    # Test for a formats specific to Geogratis
                    if topic_key == "Climatology Meteorology Atmosphere":
                        topic_key = "Climatology / Meteorology / Atmosphere"

                    # Finally, validate that the topic appears in the approved Open Data list of topics.
                    # Topics also determine which subjects are used, so for valid topics, assign the relevant subjects
                    if topic_key in self.od_topics:
                        ds.topic_category.append(self.od_topics[topic_key])
                        for s in self.od_topic_subjects[topic_key]:
                            if not s in ds.subject:
                                ds.subject.append(self.od_subjects[s])
                        ds.subject.sort()

            if geo_obj_en['deleted'] == 'false':
                ds.state = 'active'
            else:
                ds.state = 'deleted'

            if 'files' in geo_obj_en:
                for res in geo_obj_en['files']:
                    new_res = MetadataResourcesModel()
                    new_res.name = res['description']
                    new_res.url = res['link']
                    new_res.size = self._convert_size(res['size'])
                    if res['type'] in self.od_resource_formats:
                        new_res.format = self.od_resource_formats[res['type']]
                    else:
                        new_res.format = 'other'
                    ds.resources.append(new_res)
            # Add a check for datasets with 0 resources
            if len(ds.resources) == 0:
                ds.state = 'missing'

            ds.maintenance_and_update_frequency = 'As Needed | Au besoin'


        # If the English record does not exist, then don't even bother. BOTH records must exist.
        else:
            ds.state = 'missing'
            return ds

        # @TODO - Finish adding the code that parses the Geogratis records and converts them a model object
        # Only French specific fields are extracted
        if not geo_obj_fr is None:
            ds.url_fra = 'http://geogratis.gc.ca/api/fr/nrcan-rncan/ess-sst/{0}'.format(geo_obj_fr['id'])
            ds.title_fra = geo_obj_fr['title']
            ds.notes_fra = geo_obj_fr['summary']
            if 'citation' in geo_obj_fr:
                ds.data_series_name_fra = geo_obj_fr['citation']['series']

            # Keywords are extracted from two sources in the Geogratis record: the gc:subject category
            # and the keywords list
            fr_keywords = []
            fr_subjects = self._get_category(geo_obj_fr['categories'], 'urn:gc:subject')
            for term in fr_subjects:
                fr_keywords.append(self._clean_keyword(term['label']))
            ds.keywords_fra = self._extract_keywords(geo_obj_fr.get('keywords', []), fr_keywords)

            if ('citation' in geo_obj_fr) and ('seriesIssue' in geo_obj_fr['citation']):
                ds.data_series_issue_identification_fra = geo_obj_fr['citation']['seriesIssue']
            if 'files' in geo_obj_fr:
                if len(geo_obj_fr['files']) == len(ds.resources):
                    i = 0
                    for res in geo_obj_fr['files']:
                        ds.resources[i].name_fra = res['description']
                        i += 1
        ds.resources.sort(key=lambda r: r.url)
        return ds


    def _get_category(self, categories, cat_type):
        """Retrieve the category from the dataset"""
        category = []
        for cat in categories:
            if cat['type'] == cat_type:
                category = cat['terms']
                break
        return category


    def _extract_keywords(self, keywords, base_keywords):
        """Extract the keywords from keyword hierarchies

        Obtain a string with comma-separated keywords. For some NRCAN products it is necessary to
        strip away keyword hierarchy: e.g. for "one > two > three" should only be "three".

        """
        for keyword in keywords:
            words = keyword.split('>')
            if len(words) > 0:
                last_word = words.pop()
                last_word = self._clean_keyword(last_word)
                base_keywords.append(last_word.strip())
        base_keywords = list(set(base_keywords))
        base_keywords.sort()
        return base_keywords


    def _clean_keyword(self, keyword):
        """Clean up formatting on the keywords"""
        keyword = keyword.strip().replace("/", " - ")
        keyword = keyword.replace("(", "- ").replace(")", "") # change "one (two)" to "one - two"
        keyword = keyword.replace("[", "- ").replace("]", "") # change "one [two]" to "one - two"
        keyword = keyword.replace("+", "")                    # Get rid of plus signs
        keyword = keyword.lower().strip()
        return keyword

    def _convert_size(self, filesize):
        """Take a Geogratis file size string (e.g. 1.25 MB) and convert into a number of bytes in base 10"""
        byte_size = 0
        try:
            parts = filesize.split()
            num = float(parts[0])
            if parts[1] == 'KB':
                num *= 1024
            elif parts[1] == 'MB':
                num *= 1048576
            elif parts[1] == 'GB':
                num *= 1073741824
            else:
                num = 0
            byte_size = int(round(num, -1))
        except ValueError:
            return 0
        return byte_size

    def create_model_ckan(self, uuid):
        """

        :param uuid:
        :return:
        """
        ini_config = ConfigParser()
        ini_config.read('harvester.ini')
        remote_url = ini_config.get('ckan', 'ckan.remote_portal')

        ckansite = ckanapi.RemoteCKAN(remote_url)
        package = None
        try:
            package = ckansite.action.package_show(id=uuid)
        except ckanapi.NotFound, n:
            logging.warning('Dataset %s not found on CKAN site', uuid)
        except Exception, e:
            logging.error(e.message)
        ckan_json = None
        if not package is None:
            try:
                ckan_json = self.convert_ckan_json(package)
            except Exception, e:
                logging.error(e.message)
        return ckan_json


    def convert_ckan_json(self, ckan_obj):

        ds = MetadataDatasetModel()

        ds.id = ckan_obj['id']
        ds.url = 'http://data.gc.ca/data/en/dataset/{0}'.format(ckan_obj['id'])
        ds.url_fra = 'http://data.gc.ca/data/fr/dataset/{0}'.format(ckan_obj['id'])
        ds.title = ckan_obj['title']
        ds.title_fra = ckan_obj['title_fra']
        ds.notes = ckan_obj['notes']
        ds.notes_fra = ckan_obj['notes_fra']
        if 'date_modified' in ckan_obj:
            ds.date_modified = ckan_obj['date_modified'][0:10] # only using the date portion
        ds.data_series_name = ckan_obj['data_series_name']
        ds.data_series_name_fra = ckan_obj['data_series_name_fra']
        ds.keywords = [k.lower().strip() for k in ckan_obj['keywords'].split(',')]
        ds.keywords = list(set(ds.keywords))
        ds.keywords.sort()
        ds.keywords_fra = [k.lower() for k in ckan_obj['keywords_fra'].split(',')]
        ds.keywords_fra = list(set(ds.keywords_fra))
        ds.keywords_fra.sort()
        ds.spatial = ckan_obj['spatial']
        ds.presentation_form = ckan_obj['presentation_form']
        ds.digital_object_identifier = ckan_obj['digital_object_identifier']
        ds.geographic_region = [r.lower() for r in ckan_obj['geographic_region']]
        ds.data_series_issue_identification = ckan_obj['data_series_issue_identification']
        ds.data_series_issue_identification_fra = ckan_obj['data_series_issue_identification_fra']
        ds.browse_graphic_url = ckan_obj['browse_graphic_url']
        ds.topic_category = [t.lower() for t in ckan_obj['topic_category']]
        ds.subject = ckan_obj['subject']
        ds.subject.sort()
        ds.state = ckan_obj['state']
        ds.resources = []
        for r in ckan_obj['resources']:
            new_res = MetadataResourcesModel()
            new_res.url = r['url']
            new_res.name = r['name']
            new_res.name_fra = r['name_fra']
            new_res.format = r['format']
            ds.resources.append(new_res)
        ds.resources.sort(key=lambda r: r.url)

        return ds
