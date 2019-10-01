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
from multiprocessing import Pool, cpu_count
from time import time

# related third party imports
from lxml import etree

# local application/library specific imports
from patent_parser.xml_handler_V4 import SimpleXMLHandler
from patent_parser.xml_handler_V2_5 import SimpleXMLHandlerV25
from patent_parser.aps_parser import APSFileHandler, APSHandler


class XMLFileHandler:
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
        if not name.endswith('.xml') and not name.endswith('.sgml') and not name.endswith('.txt'):
            continue
        # determine hadler for the document format based on file naming
        if os.path.basename(name).startswith('pftaps'):
            f = APSFileHandler(zfile.open(name, 'r'))
        else:
            f = XMLFileHandler(zfile.open(name, 'r'))

        for elem in f.list_xmls():
            # debug start
            # z = codecs.open('debug.txt', 'w', 'utf-8')
            # z.write(elem.getvalue())
            # z.close()
            # debug end
            if os.path.basename(name).startswith('pftaps'):
                myparser = APSHandler()
                myparser.feed(elem.getvalue())
                result = myparser.output()
            else:
                if os.path.basename(name).startswith('pg0'):
                    xml_handler = SimpleXMLHandlerV25()
                else:
                    xml_handler = SimpleXMLHandler()

                myparser = etree.XMLParser(target=xml_handler, resolve_entities=False, load_dtd=False, recover=True)
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
            # determine hadler for the document format based on file naming
            if os.path.basename(file_in).startswith('pftaps'):
                self.parser = APSHandler()
                for line in f.readlines():
                    self.parser.feed(line)
                result = self.parser.output()
            else:
                if os.path.basename(file_in).startswith('pg0'):
                    xml_handler = SimpleXMLHandlerV25()
                else:
                    xml_handler = SimpleXMLHandler()

                self.parser = etree.XMLParser(target=xml_handler, resolve_entities=False, load_dtd=dtd, recover=True)
                result = etree.parse(f, self.parser)
            pickle.dump(result, write_buffer, pickle.HIGHEST_PROTOCOL)
            write_buffer.flush()
            elapsed = (time() - start)
            print(str(elapsed) + ' seconds elapsed in total.')

    @staticmethod
    def decode_patent_zip(file_in, file_out):
        decode_jobs = []
        decode_job = {'file_out': file_out, 'zip_file': file_in}
        decode_jobs.append(decode_job)
        results = _run_decoding_job(decode_job)
        print(results)

    @staticmethod
    def decode_patent_folder(folder_in, folder_out):
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

            pool = Pool(processes=cpu_count() - 1)
            results = pool.map(_run_decoding_job, decode_jobs)
            pool.close()
            pool.join()
            print(results)
