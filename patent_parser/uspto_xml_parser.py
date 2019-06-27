"""
References
----------
based on code from Dennis Hoppe.
https://github.com/hopped/uspto-patents-parsing-tools

"""

# standard library imports
import os
import zipfile
import io
from io import BytesIO
import pickle
import datetime
from multiprocessing import Pool, cpu_count
from time import time

# related third party imports
from lxml import etree

# local application/library specific imports

class FileHandler:
    def __init__(self, zfile):
        self.zfile = zfile

    def read_line(self):
        return self.zfile.readline()

    def list_xmls(self):
        output = BytesIO()
        line = self.read_line()
        output.write(line)
        line = self.read_line()
        while line is not b'':
            if b'<?xml version="1.0" encoding="UTF-8"?>' in line:
                line = line.replace(b'<?xml version="1.0" encoding="UTF-8"?>', b'')
                output.write(line)
                output.seek(0)
                yield output
                output = BytesIO()
                output.write(b'<?xml version="1.0" encoding="UTF-8"?>')
            elif b'<?xml version="1.0"?>' in line:
                line = line.replace(b'<?xml version="1.0"?>', b'')
                output.write(line)
                output.seek(0)
                yield output
                output = BytesIO()
                output.write(b'<?xml version="1.0"?>')
            else:
                output.write(line)
            try:
                line = self.read_line()
            except StopIteration:
                break
        output.seek(0)
        yield output


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
            self.document_id = self.currentContent['country'] + self.currentContent['doc-number'] + self.currentContent[
                'kind']
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

        elif tag == 'classifications-ipcr':
            self.currentPatent['internationalClassifications'] = self.internationalClassifications
            self.internationalClassifications = []

        elif tag == 'classifications-cpc':
            self.currentPatent['cooperativeClassifications'] = self.cooperativeClassifications
            self.internationalClassifications = []

    def close(self):
        return self.currentPatent


def _run_decoding_job(job):
    file_out = job['file_out']
    zip_file = job['zip_file']
    write_buffer = io.open(file_out, 'wb')
    try:
        zfile = zipfile.ZipFile(zip_file, 'r')
    except zipfile.BadZipfile:
        return 'error'
    print('process ' + str(zip_file))
    patents_within_document = 0
    relevant_documents = 0
    classification_matches = 0
    cooperative_classification_matches = 0
    patents = []
    start = time()
    for name in zfile.namelist():
        if not name.endswith('.xml') and not name.endswith('.sgml'):
            continue
        f = FileHandler(zfile.open(name, 'r'))
        for elem in f.list_xmls():
            # debug start
            # z = codecs.open('debug.txt', 'w', 'utf-8')
            # z.write(elem.getvalue())
            # z.close()
            # debug end
            myparser = etree.XMLParser(target=SimpleXMLHandler(), resolve_entities=False, load_dtd=False)
            result = etree.parse(elem, myparser)
            relevant_classification = False
            for ipc in result.get('internationalClassifications', []):
                if ipc[0] == 'H' or ipc[0] == 'G' or ipc[0] == 'Y':
                    relevant_classification = True
                    classification_matches += 1
                    break

            for ipc in result.get('cooperativeClassifications', []):
                if ipc[0] == 'H' or ipc[0] == 'G' or ipc[0] == 'Y':
                    relevant_classification = True
                    cooperative_classification_matches += 1
                    break

            if result.get('patentType', '') == 'utility' and relevant_classification:
                patents.append(result)
                relevant_documents += 1

            write_buffer.flush()
            patents_within_document = patents_within_document + 1
        zfile.close()
    pickle.dump(patents, write_buffer, pickle.HIGHEST_PROTOCOL)
    print(file_out + ': ' + str(patents_within_document) + ' patents parsed. ' + str(
        relevant_documents) + ' patents exported.   '
          + str(classification_matches) + ' patents with matching international classifications.   '
          + str(cooperative_classification_matches) + ' patents with matching cooperative classifications')
    elapsed = (time() - start)
    print(str(elapsed) + ' seconds elapsed in total.')
    return file_out


class PatentExtractor:
    def __init__(self):
        self.patents_count = 0
        self.parser = None

    def decode_patent_file(self, file_in, file_out):
        dtd = False
        write_buffer = io.open(file_out, 'wb')
        self.patents_count = 0

        if file_in is not None:
            start = time()
            f = open(file_in, 'r')
            self.parser = etree.XMLParser(target=SimpleXMLHandler(), resolve_entities=False, load_dtd=dtd)
            result = etree.parse(f, self.parser)
            pickle.dump(result, write_buffer, pickle.HIGHEST_PROTOCOL)
            write_buffer.flush()
            elapsed = (time() - start)
            print(str(elapsed) + ' seconds elapsed in total.')

    def decode_patent_folder(self, folder_in, folder_out):

        if folder_in is not None:
            decode_jobs = []
            for zip_file in os.listdir(folder_in):
                file_out = folder_out + '/' + zip_file.replace('.zip', '')
                zip_file = folder_in + '/' + zip_file
                if not zip_file.endswith('zip'):
                    continue
                if zip_file.endswith('pa030501.zip'):
                    continue  # that file is corrupt
                decode_job = {'file_out': file_out, 'zip_file': zip_file}
                decode_jobs.append(decode_job)

            pool = Pool(processes=cpu_count()-1)
            results = pool.map(_run_decoding_job, decode_jobs)
            pool.close()
            pool.join()
            print(results)
