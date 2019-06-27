from patent_parser.uspto_xml_parser import PatentExtractor

if __name__ == '__main__':
    extractor = PatentExtractor()

    extractor.decode_patent_folder('E:/patent_bulks/', 'E:/parsed_patents/')