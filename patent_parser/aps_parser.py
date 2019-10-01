# standard library imports
import datetime
from io import StringIO, TextIOWrapper


class APSFileHandler:

    def __init__(self, zip_file):
        self.patents_file = TextIOWrapper(zip_file)

    def readline(self):
        return self.patents_file.readline()

    def list_xmls(self):
        output = StringIO()

        try:
            self.readline()
            line = self.readline()
            output.write(line)
            line = self.readline()

        except StopIteration:
            print('error')
            return

        while line is not '':
            if 'PATN' in line:
                output.seek(0)
                yield output
                output = StringIO()
                output.write(line)
            else:
                output.write(line)
            try:
                line = self.readline()
            except StopIteration:
                break
        output.seek(0)
        yield output


class APSHandler(object):

    def __init__(self):

        # states
        self.state = ''
        self.states = {'INVT': 0, 'ASSG': 0, 'CLAS': 0, 'ABST': 0,
                       'PATN': 0, 'BSUM': 0, 'DETD': 0, 'CLMS': 0,
                       'DRWD': 0, 'DCLM': 0}

        self.paragraph_codes = ['PAR', 'PAC', 'PAL', 'PA1', 'PA2', 'PA3', 'PA4', 'PA5',
                                'FNT', 'TBL', 'EQU']
        self.document_date = None
        self.patentType = ''
        self.inventors = []
        self.assignees = []
        self.internationalClassifications = []
        self.claims = ''
        self.abstract = ''
        self.description = ''
        self.currentPatent = {}

    def clear(self):
        self.inventors = []
        self.assignees = []
        self.internationalClassifications = []
        self.claims = ''
        self.abstract = ''
        self.description = ''
        self.currentPatent = {}

    def feed(self, text):
        lines = text.split('\n')
        for line in lines:
            line = line.rstrip()
            data = ''
            if ' ' in line:  # data field
                data = line[3:]
                if len(data) == 0 or data[0] is not ' ':
                    data = line[1:]
                data = data.lstrip()
            else:  # change state
                if line in self.states:
                    self.state = line
                    if line == 'PATN':
                        self.clear()

            if line.startswith('WKU'):
                if self.state == 'PATN':
                    self.currentPatent['publicationNumber'] = data

            if line.startswith('APN'):
                if self.state == 'PATN':
                    self.currentPatent['applicationNumber'] = data

            if line.startswith('APT'):
                if self.state == 'PATN':
                    if data.startswith('4'):
                        self.patentType = "design"
                    elif data.startswith('6'):
                        self.patentType = "plant"
                    else:
                        self.patentType = 'utility'
                    self.currentPatent['patentType'] = self.patentType

            if line.startswith('APD'):
                if self.state == 'PATN':
                    try:
                        self.document_date = datetime.datetime.strptime(data, "%Y%m%d").date()
                    except ValueError:
                        self.document_date = datetime.date(1337, 1, 1)
                    self.currentPatent['date'] = self.document_date

            if line.startswith('ISD'):
                if self.state == 'PATN':
                    try:
                        self.document_date = datetime.datetime.strptime(data, "%Y%m%d").date()
                    except ValueError:
                        self.document_date = datetime.date(1337, 1, 1)
                    self.currentPatent['publicationDate'] = self.document_date

            elif line.startswith('TTL'):
                if self.state == 'PATN':
                    self.currentPatent['title'] = data

            elif line.startswith('NAM'):
                if self.state == 'INVT':
                    tokens = data.split(';')
                    if len(tokens) > 1:
                        name = tokens[1].strip() + ' ' + tokens[0].strip()
                    else:
                        name = tokens[0].strip()
                    self.inventors.append(name)

                elif self.states['ASSG'] == 1:
                    self.assignees.append(data)

            elif line.startswith('ICL'):
                if self.state == 'CLAS':
                    self.internationalClassifications.append(data)

            else:
                is_paragrapf = self.is_paragraph_code(line)
                if is_paragrapf or line[:4] == '    ':
                    self.add_paragraph_data(data, is_paragrapf)

    def is_paragraph_code(self, id_code):
        return id_code[:3] in self.paragraph_codes

    def add_paragraph_data(self, data, with_paragraph):
        if self.state == 'BSUM' or self.state == 'DETD' or self.state == 'DRWD':
            self.description = APSHandler.append_data(self.description, data, with_paragraph)
        elif self.state == 'ABST':
            self.abstract = APSHandler.append_data(self.abstract, data, with_paragraph)
        elif self.state == 'CLMS' or self.state == 'DCLM':
            self.claims = APSHandler.append_data(self.claims, data, with_paragraph)

    @staticmethod
    def append_data(current_data, new_data, with_paragraph):
        if with_paragraph and len(current_data) > 0:
            current_data = current_data + '\n' + new_data
        else:
            current_data = current_data + ' ' + new_data
        return current_data

    def output(self):
        self.currentPatent['abstract'] = self.abstract.strip()
        self.currentPatent['description'] = self.description.strip()
        self.currentPatent['claims'] = self.claims.strip()
        self.currentPatent['inventors'] = self.inventors
        self.currentPatent['assignees'] = self.assignees
        self.currentPatent['internationalClassifications'] = self.internationalClassifications
        return self.currentPatent
