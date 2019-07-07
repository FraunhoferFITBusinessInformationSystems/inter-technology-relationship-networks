"""This module defines methods to acquire and stream-preprocess patent data.

"""

# standard library imports
import sys

# related third party imports
from multiprocessing import Pool, cpu_count

# local application/library specific imports
from preprocessing_methods import *
from logfile import append_logfile


def stream_preprocessing(stream_processing_job):
    """This method is called by an individual worker process to preprocess a part of the data

    Parameters
    ----------
    stream_processing_job : dict
        This parameter has to be filled with items associated to the following keys
        'nodes_to_analyze', 'file_path', 'preprocessing', 'remove_stopwords', 'filter_patents_by_node'

    Returns
    -------
    assets : list
    """
    assets = []
    nodes_to_analyze = stream_processing_job.get('nodes_to_analyze')
    file_path = stream_processing_job.get('file_path')
    preprocessing = stream_processing_job.get('preprocessing')
    remove_stopwords = stream_processing_job.get('remove_stopwords')
    filter_patents_by_node = stream_processing_job.get('filter_patents_by_node')
    nlp = NLP(nodes_to_analyze)
    filtered_out = 0

    start = timeit.default_timer()

    with open(file_path, "rb") as fp:
        patent_file = pickle.load(fp)

    for patent_entry in patent_file:
        year = patent_entry.get("date").year
        title = patent_entry.get("title")
        abstract = patent_entry.get("abstract")
        claims = patent_entry.get("claims")
        description = patent_entry.get("description")
        assignees = patent_entry.get("assignees")
        ipc = patent_entry.get("internationalClassifications", "")
        cpc = patent_entry.get("cooperativeClassifications", "")

        # Sentence_tokenize
        if preprocessing == "sentences_with_lemmas":
            tokenized_title = nlp.tokenize_into_words(title)
            tokenized_abstract = nlp.tokenize_into_words(abstract)
            proceed = False

            if filter_patents_by_node:
                w = tokenized_title + tokenized_abstract
                proceed = nodes_to_analyze.is_node_in_text(w)

            if not filter_patents_by_node or proceed:
                title_sentences = nlp.tokenize_into_sentences(title)
                abstract_sentences = nlp.tokenize_into_sentences(abstract)
                claims_sentences = nlp.tokenize_into_sentences(claims)
                description_sentences = nlp.tokenize_into_sentences(description)

                title_sentences_with_lemmas = []
                for sentence in title_sentences:
                    lemmatized_sentence = nlp.lemmatize_words(sentence)
                    if remove_stopwords == "Nltk-Stopwords":
                        lemmatized_sentence = nlp.list_based_stopword_removal(lemmatized_sentence)
                    title_sentences_with_lemmas.append(lemmatized_sentence)

                abstract_sentences_with_lemmas = []
                for sentence in abstract_sentences:
                    lemmatized_sentence = nlp.lemmatize_words(sentence)
                    if remove_stopwords == "Nltk-Stopwords":
                        lemmatized_sentence = nlp.list_based_stopword_removal(lemmatized_sentence)
                    abstract_sentences_with_lemmas.append(lemmatized_sentence)

                claims_sentences_with_lemmas = []
                for sentence in claims_sentences:
                    lemmatized_sentence = nlp.lemmatize_words(sentence)
                    if remove_stopwords == "Nltk-Stopwords":
                        lemmatized_sentence = nlp.list_based_stopword_removal(lemmatized_sentence)
                    claims_sentences_with_lemmas.append(lemmatized_sentence)

                description_sentences_with_lemmas = []
                for sentence in description_sentences:
                    lemmatized_sentence = nlp.lemmatize_words(sentence)
                    if remove_stopwords == "Nltk-Stopwords":
                        lemmatized_sentence = nlp.list_based_stopword_removal(lemmatized_sentence)
                    description_sentences_with_lemmas.append(lemmatized_sentence)

                asset = SentenceLemmatizedPatentAsset(
                    year=year, nodes=nodes_to_analyze, title_sentences=title_sentences_with_lemmas,
                    abstract_sentences=abstract_sentences_with_lemmas,
                    claims_sentences=claims_sentences_with_lemmas,
                    description_sentences=description_sentences_with_lemmas,
                    assignees=assignees, cpc=cpc, ipc=ipc)

                if asset.matches_any_node():
                    assets.append(asset)
                else:
                    filtered_out = filtered_out + 1

        # Word Tokenize
        elif preprocessing == "word_tokenize":

            tokenized_title = nlp.tokenize_into_words(title)
            tokenized_abstract = nlp.tokenize_into_words(abstract)
            proceed = False

            if filter_patents_by_node:
                w = tokenized_title + tokenized_abstract
                proceed = nodes_to_analyze.is_node_in_text(w)

            if not filter_patents_by_node or proceed:
                tokenized_claims = nlp.tokenize_into_words(claims)
                tokenized_description = nlp.tokenize_into_words(description)

                if remove_stopwords == "Nltk-Stopwords":
                    tokenized_title = nlp.list_based_stopword_removal(tokenized_title)
                    tokenized_abstract = nlp.list_based_stopword_removal(tokenized_abstract)
                    tokenized_claims = nlp.list_based_stopword_removal(tokenized_claims)
                    tokenized_description = nlp.list_based_stopword_removal(tokenized_description)

                asset = WordTokenizedPatentAsset(year=year, nodes=nodes_to_analyze,
                                             tokenized_title=tokenized_title,
                                             tokenized_abstract=tokenized_abstract,
                                             tokenized_claims=tokenized_claims,
                                             tokenized_description=tokenized_description,
                                             assignees=assignees, cpc=cpc, ipc=ipc)
                if asset.matches_any_node():
                    assets.append(asset)
                else:
                    filtered_out = filtered_out + 1

        # Pos Tagging (to do)
        elif preprocessing == "pos_tag":
            pos_tagged_title = nlp.pos_tag_words(title)
            pos_tagged_abstract = nlp.pos_tag_words(abstract)
            pos_tagged_claims = nlp.pos_tag_words(claims)
            pos_tagged_description = nlp.pos_tag_words(description)
            if remove_stopwords == "Nltk-Stopwords":
                pos_tagged_title = nlp.list_based_stopword_removal_for_pos_tagged_words(pos_tagged_title)
                pos_tagged_abstract = nlp.list_based_stopword_removal_for_pos_tagged_words(pos_tagged_abstract)
                pos_tagged_claims = nlp.list_based_stopword_removal_for_pos_tagged_words(pos_tagged_claims)
                pos_tagged_description = nlp.list_based_stopword_removal_for_pos_tagged_words(pos_tagged_description)

            asset = PosTaggedPatentAsset(year=year, nodes=nodes_to_analyze,
                                     pos_tagged_title=pos_tagged_title,
                                     pos_tagged_abstract=pos_tagged_abstract,
                                     pos_tagged_claims=pos_tagged_claims,
                                     pos_tagged_description=pos_tagged_description,
                                     assignees=assignees, cpc=cpc, ipc=ipc)
            if asset.matches_any_node():
                assets.append(asset)
            else:
                filtered_out = filtered_out + 1

        # Lemmatize
        elif preprocessing == "lemmatize":
            tokenized_title = nlp.tokenize_into_words(title)
            tokenized_abstract = nlp.tokenize_into_words(abstract)
            proceed = False

            if filter_patents_by_node:
                w = tokenized_title + tokenized_abstract
                proceed = nodes_to_analyze.is_node_in_text(w)

            if not filter_patents_by_node or proceed:
                lemmatized_title = nlp.lemmatize_words(title)
                lemmatized_abstract = nlp.lemmatize_words(abstract)
                lemmatized_claims = nlp.lemmatize_words(claims)
                lemmatized_description = nlp.lemmatize_words(description)
                if remove_stopwords == "Nltk-Stopwords":
                    lemmatized_title = nlp.list_based_stopword_removal(lemmatized_title)
                    lemmatized_abstract = nlp.list_based_stopword_removal(lemmatized_abstract)
                    lemmatized_claims = nlp.list_based_stopword_removal(lemmatized_claims)
                    lemmatized_description = nlp.list_based_stopword_removal(lemmatized_description)

                asset = LemmatizedPatentAsset(year=year, nodes=nodes_to_analyze,
                                          lemmatized_title=lemmatized_title,
                                          lemmatized_abstract=lemmatized_abstract,
                                          lemmatized_claims=lemmatized_claims,
                                          lemmatized_description=lemmatized_description,
                                          assignees=assignees, cpc=cpc, ipc=ipc)

                if asset.matches_any_node():
                    assets.append(asset)
                else:
                    filtered_out = filtered_out + 1

        else:
            sys.exit("Pipeline should never reach this point!")

    # Logfile
    stop = timeit.default_timer()
    runtime = stop - start
    print('Finished: ' + file_path + " with: " + str(len(assets)) +
          ' Assets found in ' + str(len(patent_file)) + ' patents. Duration: ' + str(runtime) +
          ' ' + str(filtered_out) + ' assets filtered out by search expressions')
    return assets


class PatentData:
    def __init__(self, logfile_path):
        self.mwe_tokenizer = self.tagger = self.lemmatizer = self.stop_words = None
        self.logfile_path = logfile_path

    def preprocess_patent_files_from_dir(
            self, data_dir, preprocessing, remove_stopwords, nodes_to_analyze,
            filter_patents_by_node):
        """Method to extract, load and preprocess patent data parsed
        by our uspto_xml_parser with support for multiprocessing.

        Parameters
        ----------
        data_dir : str
        preprocessing : str
            Specifies which preprocessing method to apply. Supported strings:
            'word_tokenize', 'sentences_with_lemmas', 'pos_tag', and 'lemmatize'.

        remove_stopwords : str
            Specifies which stopword-list to apply. Supported strings: Nltk-Stopwords

        nodes_to_analyze: Nodes
            nodes that have to be analyzed

        filter_patents_by_node: bool

        Returns
        -------
        nothing

        """
        start = timeit.default_timer()
        stream_processing_jobs = []

        for root, dirs, files in os.walk(data_dir):
            for name in files:
                file_path = data_dir + name
                if os.path.getsize(file_path) > 0:
                    stream_processing_job = {"preprocessing": preprocessing,
                                             "remove_stopwords": remove_stopwords,
                                             "file_path": file_path,
                                             "nodes_to_analyze": nodes_to_analyze,
                                             "filter_patents_by_node": filter_patents_by_node}
                    stream_processing_jobs.append(stream_processing_job)
                else:
                    print("Empty File!")

        p = Pool(processes=cpu_count()-1, maxtasksperchild=1)
        asset_cnt = 0
        for assets in p.imap_unordered(stream_preprocessing, stream_processing_jobs):
            nodes_to_analyze.enrich_with_assets(assets)
            print("Imported " + str(len(assets)) + " assets into nodes")
            asset_cnt = asset_cnt + len(assets)
        p.close()
        p.join()

        # Logfile
        stop = timeit.default_timer()
        runtime = stop - start
        event_title = "Load and preprocess Patent Data from Directory"
        event_description = \
            "Importing " + str(asset_cnt) + " patents from directory " \
                                              "into assetlist." \
            + " Preprocessing = " + str(preprocessing)
        append_logfile(logfile_path=self.logfile_path,
                       event_title=event_title,
                       event_description=event_description,
                       runtime=runtime)
