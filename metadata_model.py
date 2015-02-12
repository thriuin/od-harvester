__author__ = 'Statistics Canada'
__copyright__ = 'Crown Copyright'
__license__ = 'MIT'

import simplejson as json

class MetadataDatasetModel():

    id = ''
    url = ''
    url_fra = ''
    title = ''
    title_fra = ''
    notes = ''
    notes_fra = ''
    date_modified = ''
    data_series_name = ''
    data_series_name_fra = ''
    keywords = ''
    keywords_fra = ''
    spatial = ''
    presentation_form = ''
    digital_object_identifier = ''
    geographic_region = []
    data_series_issue_identification = ''
    data_series_issue_identification_fra = ''
    browse_graphic_url = ''
    topic_category = []
    subject = []
    state = ''
    resources = []
    type = u'dataset'
    spatial_representation_type = ''
    department_number = ''
    attribution = ''
    attribution_fra = ''
    time_period_coverage_start = ''
    time_period_coverage_end = ''

    # These fields are not used when comparing datasets

    catalog_type = u'Data | Donn\u00e9es'
    author_email = u'open-ouvert@tbs-sct.gc.ca'
    license_id = u'ca-ogl-lgo'
    ds_type = u'dataset'
    ready_to_publish = False
    portal_release_date = ''
    owner_org = ''
    maintenance_and_update_frequency = ''
    endpoint_url = ''
    endpoint_url_fra = ''
    language = ''
    date_published = ''

    # Class fields
    topic_choices= []

    def __init__(self):

        self.geographic_region = []
        self.topic_category = []
        self.subject = []
        self.resources = []

    def as_dict(self):

        my_resources = []
        for r in self.resources:
            my_resources.append(r.as_dict())

        me = {'attribution': u'Contains information licensed under the Open Government Licence \u2013 Canada.',
              'attribution_fra': u'Contient des informations autoris\u00e9es sous la Licence du gouvernement ouvert- Canada',
              'author_email': self.author_email,
              'browse_graphic_url': self.browse_graphic_url,
              'catalog_type': self.catalog_type,
              'data_series_issue_identification': self.data_series_issue_identification,
              'data_series_issue_identification_fra': self.data_series_issue_identification_fra,
              'data_series_name': self.data_series_name,
              'data_series_name_fra': self.data_series_name_fra,
              'date_published': self.date_published,
              'digital_object_identifier': self.digital_object_identifier,
              'endpoint_url': self.endpoint_url,
              'endpoint_url_fra': self.endpoint_url_fra,
              'id': self.id,
              'keywords': ','.join(self.keywords),
              'keywords_fra': ','.join(self.keywords_fra),
              'language': self.language,
              'license_id': self.license_id,
              'maintenance_and_update_frequency': self.maintenance_and_update_frequency,
              'name': self.id,
              'notes': self.notes,
              'notes_fra': self.notes_fra,
              'owner_org': self.owner_org,
              'portal_release_date': self.portal_release_date,
              'presentation_form': self.presentation_form,
              'ready_to_publish': self.ready_to_publish,
              'regions': self.geographic_region,
              'spatial': self.spatial,
              'spatial_representation_type': self.spatial_representation_type,
              'state': self.state,
              'subject': self.subject,
              'title': self.title,
              'title_fra': self.title_fra,
              'topic_category': self.topic_category,
              'type': self.ds_type,
              'url': self.url,
              'url_fra': self.url_fra}
        if len(my_resources) > 0:
            me['resources'] = my_resources

        return me

    def equals(self, other):
        is_equal = True
        if self.id != other.id: is_equal = False
        if self.url != other.url: is_equal = False
        if self.url_fra != other.url_fra: is_equal = False
        if self.title != other.title: is_equal = False
        if self.title_fra != other.title_fra: is_equal = False
        if self.notes != other.notes: is_equal = False
        if self.notes_fra != other.notes_fra: is_equal = False
        if self.date_modified != other.date_modified: is_equal = False
        if self.data_series_name != other.data_series_name: is_equal = False
        if self.data_series_name_fra != other.data_series_name_fra: is_equal = False
        if self.spatial != other.spatial: is_equal = False
        if self.presentation_form != other.presentation_form: is_equal = False
        if self.digital_object_identifier != other.digital_object_identifier: is_equal = False
        if self.data_series_issue_identification != other.data_series_issue_identification: is_equal = False
        if self.data_series_issue_identification_fra != other.data_series_issue_identification_fra: is_equal = False
        if self.browse_graphic_url != other.browse_graphic_url: is_equal = False
        if self.state != other.state: is_equal = False

        if len(self.keywords) == len(other.keywords):
            if not compare_list(self.keywords, other.keywords):
                is_equal = False
        else:
            is_equal = False

        if len(self.keywords_fra) == len(other.keywords_fra):
            if not compare_list(self.keywords_fra, other.keywords_fra):
                is_equal = False
        else:
            is_equal = False

        if len(self.geographic_region) == len(other.geographic_region):
            if not compare_list(self.geographic_region, other.geographic_region):
                is_equal = False
        else:
            is_equal = False

        if len(self.topic_category) == len(other.topic_category):
            if not compare_list(self.topic_category, other.topic_category):
                is_equal = False
        else:
            is_equal = False

        if len(self.subject) == len(other.subject):
            if not compare_list(self.subject, other.subject):
                is_equal = False
        else:
            is_equal = False

        if len(self.resources) == len(other.resources):
            for i in range(0, len(self.resources) - 1):
                if not self.resources[i].equals(other.resources[i]):
                    is_equal = False
                    break
        else:
            is_equal = False

        return is_equal

    def compare(self, other, self_label='Source', other_label='Other'):
        diff_list = []
        if self.id != other.id:
            diff_list.append(u"ID: \t{2} [{0}], \t{3} [{1}]".format(self.id, other.id, self_label, other_label))
        if self.url!= other.url:
            diff_list.append(u"URL: \t{2} [{0}], \t{3} [{1}]".format(self.url, other.url, self_label, other_label))
        if self.url_fra != other.url_fra:
            diff_list.append(u"URL (FR): \t{2} [{0}], \t{3} [{1}]".format(self.url_fra, other.url_fra, 
                                                                          self_label, other_label))
        if self.title != other.title:
            diff_list.append(u"Title: \t{2} [{0}], \t{3} [{1}]".format(self.title, other.title, 
                                                                       self_label, other_label))
        if self.title_fra != other.title_fra:
            diff_list.append(u"Title (FR): \t{2} [{0}], \t{3} [{1}]".format(self.title_fra, other.title_fra, 
                                                                            self_label, other_label))
        if self.notes != other.notes:
            diff_list.append(u"Notes: \t{2} [{0}], \t{3} [{1}]".format(self.notes, other.notes, 
                                                                       self_label, other_label))
        if self.notes_fra != other.notes_fra:
            diff_list.append(u"Notes (FR): \t{2} [{0}], \t{3} [{1}]".format(self.notes_fra, other.notes_fra, 
                                                                            self_label, other_label))
        if self.date_modified != other.date_modified:
            diff_list.append(u"Date: \t{2} [{0}], \t{3} [{1}]".format(self.date_modified, other.date_modified, 
                                                                      self_label, other_label))
        if self.data_series_name != other.data_series_name:
            diff_list.append(u"DSN: \t{2} [{0}], \t{3} [{1}]".format(self.data_series_name,
                                                                     other.data_series_name,
                                                                     self_label, other_label))
        if self.data_series_name_fra != other.data_series_name_fra:
            diff_list.append(u"DSN (FR): \t{2} [{0}], \t{3} [{1}]".format(self.data_series_name_fra,
                                                                          other.data_series_name_fra,
                                                                          self_label, other_label))
        if self.spatial != other.spatial:
            diff_list.append(u"Spatial: \t{2} [{0}], \t{3} [{1}]".format(self.spatial, other.spatial, 
                                                                         self_label, other_label))
        if self.presentation_form != other.presentation_form:
            diff_list.append(u"PForm: \t{2} [{0}], \t{3} [{1}]".format(self.presentation_form,
                                                                       other.presentation_form,
                                                                       self_label, other_label))
        if self.digital_object_identifier != other.digital_object_identifier:
            diff_list.append(u"DOI: \t{2} [{0}], \t{3} [{1}]".format(self.digital_object_identifier,
                                                                     other.digital_object_identifier,
                                                                     self_label, other_label))
        if self.data_series_issue_identification != other.data_series_issue_identification:
            diff_list.append(u"DSII: \t{2} [{0}], \t{3} [{1}]".format(self.data_series_issue_identification,
                                                                      other.data_series_issue_identification,
                                                                      self_label, other_label))
        if self.data_series_issue_identification_fra != other.data_series_issue_identification_fra :
            diff_list.append(u"DSII (FR): \t{2} [{0}], \t{3} [{1}]".format(
                self.data_series_issue_identification_fra, other.data_series_issue_identification_fra, 
                self_label, other_label))
        if self.browse_graphic_url != other.browse_graphic_url:
            diff_list.append(u"Graphic: \t{2} [{0}], \t{3} [{1}]".format(self.browse_graphic_url,
                                                                         other.browse_graphic_url,
                                                                         self_label, other_label))
        if self.state != other.state:
            diff_list.append(u"State: \t{2} [{0}], \t{3} [{1}]".format(self.state, other.state))
            
        if not len(self.keywords) == len(other.keywords):
            diff_list.append(u"Keywords: \tCount {2} - {0}, \tCount {3} - {1}".format(len(self.keywords),
                                                                                      len(other.keywords),
                                                                                      self_label, other_label))
        else:
            if not compare_list(self.keywords, other.keywords):
                diff_list.append(u"Keywords: \t{2} - {0}, \t{3} - {1}".format(self.keywords,
                                                                              other.keywords,
                                                                              self_label, other_label))
        if not len(self.keywords_fra) == len(other.keywords_fra):
            diff_list.append(u"Keywords (FR): \tCount {2} - {0}, \tCount {3} - {1}".format(len(self.keywords_fra),
                                                                                           len(other.keywords_fra),
                                                                                           self_label, other_label))
        elif not compare_list(self.keywords_fra, other.keywords_fra):
            diff_list.append(u"Keywords FR: \t{2} - {0}, \t{3} - {1}".format(self.keywords_fra,
                                                                             other.keywords_fra,
                                                                             self_label, other_label))            
        if not len(self.geographic_region) == len(other.geographic_region):
            diff_list.append(u"Regions: \tCount {2} - {0}, \tCount {3} - {1}".format(len(self.geographic_region),
                                                                                     len(other.geographic_region),
                                                                                     self_label, other_label))
        elif not compare_list(self.geographic_region, other.geographic_region):
                diff_list.append(u"geographic_region: \t{2} - {0}, \t{3} - {1}".format(self.geographic_region,
                                                                                       other.geographic_region,
                                                                                       self_label, other_label))
        if not len(self.topic_category) == len(other.topic_category):
            diff_list.append(u"Topics: \tCount {2} - {0}, \tCount {3} - {1}".format(len(self.topic_category),
                                                                                    len(other.topic_category),
                                                                                    self_label, other_label))
        elif not compare_list(self.topic_category, other.topic_category):
                diff_list.append(u"Topics: \t{2} - {0}, \t{3} - {1}".format(self.topic_category,
                                                                            other.topic_category,
                                                                            self_label, other_label))
        if not len(self.subject) == len(other.subject):
            diff_list.append(u"Subjects: \tCount {2} - {0}, \tCount {3} - {1}".format(len(self.subject),
                                                                                      len(other.subject),
                                                                                      self_label, other_label))
        elif not compare_list(self.subject, other.subject):
                diff_list.append(u"subject: \t{2} - {0}, \t{3} - {1}".format(self.subject,
                                                                             other.subject,
                                                                             self_label, other_label))
        if not len(self.resources) == len(other.resources):
            diff_list.append(u"Resources: \tCount {2} - {0}, \tCount {3} - {1}".format(len(self.resources),
                                                                                       len(other.resources),
                                                                                       self_label, other_label))
        else:
            for i in range(0, len(self.resources) - 1):
                if not self.resources[i].equals(other.resources[i]):
                    diff_list.append(u"Resource {0} {1}: {2}, {3}: {4}".format(i, self_label,
                                                                               self.resources[i].url,
                                                                               other_label,
                                                                               other.resources[i].url)
                                     )
                    res_diffs = self.resources[i].compare(other.resources[i], self_label, other_label)
                    for rf in res_diffs:
                        diff_list.append(rf)
        return diff_list


def compare_list(source, other):
    list_equal = True
    for i in range(0, len(source) - 1):
        if source[i] != other[i]:
            list_equal = False
            break
    return list_equal


class MetadataResourcesModel():
    name = ''
    name_fra = ''
    url = ''
    format = ''
    resource_type = ''
    size = 0
    language = ''

    def __init__(self, res_name='', res_name_fra='', res_url='', res_format='', res_type='file', res_size=0,
                 res_language='eng; CAN | fra; CAN'):
        """

        :type res_format: str
        """
        self.name = res_name
        self.name_fra = res_name_fra
        self.url = res_url
        self.format = res_format
        self.resource_type = res_type
        self.size = res_size
        self.language = res_language


    def as_dict(self):
        me = {}
        me['format'] = self.format
        me['name'] = self.name
        me['name_fra'] = self.name_fra
        me['language'] = self.language
        me['url'] = self.url
        me['resource_type'] = self.resource_type
        return me

    def equals(self, other):

        if self.name != other.name or \
            self.name_fra != other.name_fra or \
            self.url != other.url or \
            self.format != other.format or \
            self.resource_type != other.resource_type or \
            self.size != other.size or \
            self.language != other.language:
            return False
        else:
            return True

    def compare(self, other, self_label='Source', other_label='Other'):

        diff_list = []
        if self.name != other.name:
            diff_list.append(u"Name: \t{2} [{0}], \t{3} [{1}]".format(self.name, other.name, self_label,
                                                                           other_label))
        if self.name_fra != other.name_fra:
            diff_list.append(u"Name (FR): \t{2} [{0}], \t{3} [{1}]".format(self.name_fra, other.name_fra,
                                                                                self_label, other_label))
        if self.url != other.url:
            diff_list.append(u"URL: \t{2} [{0}], \t{3} [{1}]".format(self.url, other.url, self_label,
                                                                          other_label))
        if self.format != other.format:
            diff_list.append(u"Form: \t{2} [{0}], \t{3} [{1}]".format(self.format, other.format, self_label,
                                                                           other_label))
        if self.resource_type != other.resource_type:
            diff_list.append(u"Type: \t{2} [{0}], \t{3} [{1}]".format(self.resource_type, other.resource_type,
                                                                           self_label, other_label))
        if self.size != other.size:
            diff_list.append(u"Size: \t{2} [{0}], \t{3} [{1}]".format(self.size, other.size, self_label,
                                                                           other_label))
        if self.language != other.language:
            diff_list.append(u"Language: \t{2} [{0}], \t{3} [{1}]".format(self.language, other.language,
                                                                               self_label, other_label))
        return diff_list

