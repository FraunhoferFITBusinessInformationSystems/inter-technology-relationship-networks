# standard library imports
import datetime


class SimpleXMLHandler(object):
    def __init__(self):
        self.inventors = self.assignees = self.internationalClassifications = self.cooperativeClassifications = \
            self.patentType = self.description = self.document_id = self.document_date = self.personName = \
            self.orgName = self.classification = None
        self.currentTag = ''
        self.currentContent = {}
        self.claims = ''
        self.abstract = ''
        self.currentPatent = {}
        self.descriptionActive = False
        self.abstractActive = False
        self.claimsActive = False
        self.citationActive = False
        self.tagReplacements = {'organization-name': 'orgname',
                                'applicants': 'inventors',
                                'country-code': 'country',
                                'classification-ipcr': 'classification-ipc-primary',
                                'main-classification': 'subgroup',
                                'classification-national': 'classification-us-primary',
                                'title-of-invention': 'invention-title',
                                'subdoc-abstract': 'abstract',
                                'document-date': 'date'}

        self.tagToField = {'kind': 'kind',
                           'country': 'country',
                           'doc-number': 'doc-number',
                           'date': 'date',
                           'publication-reference': 'publication-reference',
                           'application-reference': 'application-reference',
                           'invention-title': 'invention-title',
                           'last-name': 'last-name',
                           'first-name': 'first-name',
                           'given-name': 'given-name',
                           'family-name': 'family-name',
                           'orgname': 'orgname',
                           'addressbook': 'addressbook',
                           'inventor': 'inventor',
                           'inventors': 'inventors',
                           'assignee': 'assignee',
                           'assignees': 'assignees',
                           'classification-level': 'classification-level',
                           'section': 'section',
                           'class': 'class',
                           'subclass': 'subclass',
                           'main-group': 'main-group',
                           'subgroup': 'subgroup',
                           'symbol-position': 'symbol-position',
                           'classification-value': 'classification-value'
                           }

    def start(self, tag, attributes):
        if tag == 'us-patent-grant':
            self.currentPatent = {}
            self.currentContent = {}
            self.inventors = []
            self.assignees = []
            self.internationalClassifications = []
            self.cooperativeClassifications = []
            self.descriptionActive = False
            self.abstractActive = False
            self.claimsActive = False
            self.citationActive = False

        self.currentTag = self.tagToField.get(self.tagReplacements.get(tag, tag), '')
        if tag == 'application-reference':
            self.patentType = attributes.get('appl-type', 'undef')
        elif tag == 'claims':
            self.claimsActive = True
            self.claims = ''
        elif tag == 'abstract':
            self.abstractActive = True
            self.abstract = ''
        elif tag == 'description':
            self.descriptionActive = True
            self.description = ''
        elif tag == 'citation':
            self.citationActive = True

    def data(self, data):
        if self.descriptionActive:
            self.description = self.description + data
        elif self.abstractActive:
            self.abstract = self.abstract + data
        elif self.claimsActive:
            if not (self.claims.endswith('\n') and data == '\n'):
                self.claims = self.claims + data
        elif len(self.currentTag) > 0 and data != '\n':
            self.currentContent[self.currentTag] = data

    def end(self, tag):
        if tag == 'document-id':
            self.document_id = self.currentContent['country'] + self.currentContent['doc-number'] \
                               + self.currentContent['kind']
            try:
                self.document_date = datetime.datetime.strptime(self.currentContent['date'], "%Y%m%d").date()
            except:
                self.document_date = datetime.date(1337, 1, 1)

        elif tag == 'publication-reference':
            self.currentPatent['publicationNumber'] = self.document_id
            self.currentPatent['publicationDate'] = self.document_date

        elif tag == 'application-reference':
            self.currentPatent['applicationNumber'] = self.document_id
            self.currentPatent['date'] = self.document_date
            self.currentPatent['patentType'] = self.patentType

        elif tag == 'invention-title':
            self.currentPatent['title'] = self.currentContent.get(tag, '')

        elif tag == 'addressbook':
            self.personName = self.currentContent.get('first-name', '')
            if self.currentContent.__contains__('last-name'):
                if self.personName != '':
                    self.personName += ' '
                self.personName += self.currentContent['last-name']
            self.orgName = self.currentContent.get('orgname', '')

        elif tag == 'inventor':
            self.inventors.append(self.personName)

        elif tag == 'inventors':
            self.currentPatent['inventors'] = self.inventors

        elif tag == 'assignee':
            if self.orgName is not None and len(self.orgName) > 0:
                self.assignees.append(self.orgName)
            else:
                self.assignees.append(self.personName)

        elif tag == 'assignees':
            self.currentPatent['assignees'] = self.assignees

        elif tag == 'abstract':
            self.currentPatent['abstract'] = self.abstract.strip()
            self.abstractActive = False

        elif tag == 'description':
            self.currentPatent['description'] = self.description.strip()
            self.descriptionActive = False

        elif tag == 'claims':
            self.currentPatent['claims'] = self.claims.strip()
            self.claimsActive = False

        elif tag == 'classification-ipcr':
            self.classification = self.currentContent.get('section', '') \
                                  + self.currentContent.get('class', '') \
                                  + self.currentContent.get('subclass', '') \
                                  + self.currentContent.get('main-group', '') \
                                  + '/' + self.currentContent.get('subgroup', '')
            self.internationalClassifications.append(self.classification)

        elif tag == 'classification-cpc':
            self.classification = self.currentContent.get('section', '') \
                                  + self.currentContent.get('class', '') \
                                  + self.currentContent.get('subclass', '') \
                                  + self.currentContent.get('main-group', '') \
                                  + '/' + self.currentContent.get('subgroup', '')
            self.cooperativeClassifications.append(self.classification)

        # Version 4.0 has no classification-ipcr tags: Use classification-ipc outside of citations
        # 'main-classification' has been renamed to 'subgroup' because of tag replacements
        elif tag == 'classification-ipc':
            if not self.citationActive:
                self.internationalClassifications.append(self.currentContent.get('subgroup', ''))
                self.currentPatent['internationalClassifications'] = self.internationalClassifications
                self.internationalClassifications = []

        elif tag == 'classifications-ipcr':
            self.currentPatent['internationalClassifications'] = self.internationalClassifications
            self.internationalClassifications = []

        elif tag == 'classifications-cpc':
            self.currentPatent['cooperativeClassifications'] = self.cooperativeClassifications
            self.internationalClassifications = []
        elif tag == 'citation':
            self.citationActive = False

    def close(self):
        return self.currentPatent
