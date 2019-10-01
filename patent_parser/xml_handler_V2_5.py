# standard library imports
import datetime


class SimpleXMLHandlerV25(object):

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

        self.tagToField = {'B130': 'kind',
                           'B190': 'country',
                           'B110': 'doc-number',
                           'B210': 'applicationNumber',
                           'DATE': 'date',
                           'B540': 'invention-title',
                           'SNM': 'last-name',
                           'FNM': 'first-name',
                           'TTL': 'title-name',
                           'ONM': 'orgname',
                           'B721': 'inventor',
                           'B731': 'assignee',
                           'B511': 'IPC-main',
                           'B512': 'IPC-further'
                           }

    def start(self, tag, attributes):
        if tag == 'PATDOC':
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

        if tag != 'PDAT' and tag != 'DNUM' and tag != 'STEXT':
            self.currentTag = self.tagToField.get(tag, '')
        if tag == 'SDOCL':
            self.claimsActive = True
            self.claims = ''
        elif tag == 'SDOAB':
            self.abstractActive = True
            self.abstract = ''
        elif tag == 'SDODE':
            self.descriptionActive = True
            self.description = ''

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
        if tag == 'B100':
            self.document_id = self.currentContent['country'] + self.currentContent['doc-number'] + self.currentContent[
                'kind']
            try:
                self.document_date = datetime.datetime.strptime(self.currentContent['date'], "%Y%m%d").date()
            except:
                self.document_date = datetime.date(1337, 1, 1)

            if self.currentContent['kind'].startswith('S'):
                self.patentType = "design"
            elif self.currentContent['kind'].startswith('P'):
                self.patentType = "plant"
            elif self.currentContent['kind'].startswith('M'):
                self.patentType = "medical"
            else:
                self.patentType = 'utility'

            self.currentPatent['publicationNumber'] = self.document_id
            self.currentPatent['publicationDate'] = self.document_date
            self.currentPatent['patentType'] = self.patentType

        elif tag == 'B540':
            self.currentPatent['title'] = self.currentContent.get(self.tagToField.get(tag, ''))

        elif tag == 'B200':
            try:
                self.document_date = datetime.datetime.strptime(self.currentContent['date'], "%Y%m%d").date()
            except:
                self.document_date = datetime.date(1337, 1, 1)
            self.currentPatent['date'] = self.document_date
            self.currentPatent['applicationNumber'] = self.currentContent['applicationNumber']

        elif tag == 'PARTY-US':
            self.personName = self.currentContent.get('first-name', '')
            if self.currentContent.__contains__('last-name'):
                if self.personName != '':
                    self.personName += ' '
                self.personName += self.currentContent['last-name']
            self.orgName = self.currentContent.get('orgname', '')

        elif tag == 'B721':
            self.inventors.append(self.personName)

        elif tag == 'B720':
            self.currentPatent['inventors'] = self.inventors

        elif tag == 'B731':
            if self.orgName is not None and len(self.orgName) > 0:
                self.assignees.append(self.orgName)
            else:
                self.assignees.append(self.personName)

        elif tag == 'B730':
            self.currentPatent['assignees'] = self.assignees

        elif tag == 'SDOAB':
            self.currentPatent['abstract'] = self.abstract.strip()
            self.abstractActive = False

        elif tag == 'SDODE':
            self.currentPatent['description'] = self.description.strip()
            self.descriptionActive = False

        elif tag == 'SDOCL':
            self.currentPatent['claims'] = self.claims.strip()
            self.claimsActive = False

        elif tag == 'B510':
            self.internationalClassifications = []
            if 'IPC-main' in self.currentContent:
                self.internationalClassifications.append(self.currentContent['IPC-main'])
                if 'IPC-further' in self.currentContent:
                    self.internationalClassifications.append(self.currentContent['IPC-further'])
            self.currentPatent['internationalClassifications'] = self.internationalClassifications
            self.internationalClassifications = []

    def close(self):
        return self.currentPatent
