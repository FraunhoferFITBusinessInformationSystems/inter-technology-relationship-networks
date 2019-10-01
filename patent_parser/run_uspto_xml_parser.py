from patent_parser.uspto_xml_parser import PatentExtractor

if __name__ == '__main__':
    extractor = PatentExtractor()

    extractor.decode_patent_folder('F:/patent_bulks/2000_2006/', 'F:/parsed_patents/2000_2006/')

    # extractor.decode_patent_zip('E:/patent_bulks/pg040120.zip', 'E:/parsed_patents/pg040120')

    # extractor.decode_patent_file('E:/patent_bulks/patent_patent_test/pftaps-sample.txt','E:/parsed_patents/outputFileAps.txt')
