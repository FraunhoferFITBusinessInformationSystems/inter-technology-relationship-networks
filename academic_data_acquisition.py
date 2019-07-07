"""This module defines methods to acquire and stream-preprocess academic data.

"""

# standard library imports
import os
import codecs
import sys
import itertools
from multiprocessing import Pool, cpu_count

# related third party imports
# none

# local application/library specific imports
from assets import *
from preprocessing_methods import *
from logfile import append_logfile


def stream_preprocessing(stream_processing_job):
    """This method is called by an individual worker process to preprocess a part of the data

    Parameters
    ----------
    stream_processing_job : dict
        This parameter has to be filled with items associated to the following keys
        'nodes_to_analyze', 'file_path', 'preprocessing', 'remove_stopwords'

    Returns
    -------
    assets : list
    """
    assets = []
    nodes_to_analyze = stream_processing_job.get('nodes_to_analyze')
    file_path = stream_processing_job.get('file_path')
    preprocessing = stream_processing_job.get('preprocessing')
    remove_stopwords = stream_processing_job.get('remove_stopwords')
    nlp = NLP(nodes_to_analyze)

    start = timeit.default_timer()

    wos_file = codecs.open(file_path, "r", "utf-8")
    skip_header = True
    for line in wos_file:
        if skip_header:
            skip_header = False
            continue
        spl = line.split('\t')
        year_str = spl[44]
        if len(year_str) == 0:
            continue                # year may be '' e.G. for early access papers
        year = int(year_str)
        author = spl[5]
        title = spl[8]
        abstract = spl[21]
        keywords = spl[19]

        if preprocessing == "sentences_with_lemmas":
            title_sentences = nlp.tokenize_into_sentences(title)
            abstract_sentences = nlp.tokenize_into_sentences(abstract)
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
            asset = SentenceLemmatizedAcademicAsset(year=year, nodes=nodes_to_analyze, authors=author,
                                                title_sentences=title_sentences_with_lemmas,
                                                abstract_sentences=abstract_sentences_with_lemmas,
                                                keywords=keywords)
            if asset.matches_any_node():
                assets.append(asset)

        elif preprocessing == "word_tokenize":
            tokenized_title = nlp.tokenize_into_words(title)
            tokenized_abstract = nlp.tokenize_into_words(abstract)
            if remove_stopwords == "Nltk-Stopwords":
                tokenized_title = nlp.list_based_stopword_removal(tokenized_title)
                tokenized_abstract = nlp.list_based_stopword_removal(tokenized_abstract)

            asset = WordTokenizedAcademicAsset(year=year, nodes=nodes_to_analyze, authors=author,
                                           tokenized_title=tokenized_title,
                                           tokenized_abstract=tokenized_abstract,
                                           keywords=keywords)
            if asset.matches_any_node():
                assets.append(asset)

        elif preprocessing == "pos_tag":
            pos_tagged_title = nlp.pos_tag_words(title)
            pos_tagged_abstract = nlp.pos_tag_words(abstract)
            if remove_stopwords == "Nltk-Stopwords":
                pos_tagged_title = nlp.list_based_stopword_removal_for_pos_tagged_words(pos_tagged_title)
                pos_tagged_abstract = nlp.list_based_stopword_removal_for_pos_tagged_words(pos_tagged_abstract)

            asset = PosTaggedAcademicAsset(year=year, nodes=[], authors=author,
                                       pos_tagged_title=pos_tagged_title,
                                       pos_tagged_abstract=pos_tagged_abstract,
                                       keywords=keywords)
            if asset.matches_any_node():
                assets.append(asset)

        elif preprocessing == "lemmatize":
            lemmatized_title = nlp.lemmatize_words(title)
            lemmatized_abstract = nlp.lemmatize_words(abstract)
            if remove_stopwords == "Nltk-Stopwords":
                lemmatized_title = nlp.list_based_stopword_removal(lemmatized_title)
                lemmatized_abstract = nlp.list_based_stopword_removal(lemmatized_abstract)

            asset = LemmatizedAcademicAsset(year=year, nodes=nodes_to_analyze, authors=author,
                                        lemmatized_title=lemmatized_title,
                                        lemmatized_abstract=lemmatized_abstract,
                                        keywords=keywords)
            if asset.matches_any_node():
                assets.append(asset)

        else:
            sys.exit("Pipeline should never reach this point!")
    wos_file.close()

    # Logfile
    stop = timeit.default_timer()
    runtime = stop - start
    print('Finished: ' + file_path + " with: " + str(len(assets)) + ' Assets  Duration: '
          + str(runtime))
    return assets


class AcademicData:
    def __init__(self, logfile_path):
        self.mwe_tokenizer = self.tagger = self.lemmatizer = self.stop_words = None
        self.logfile_path = logfile_path

    def preprocess_wos_articles_from_dir(self, data_dir, preprocessing,
                                         remove_stopwords, nodes_to_analyze):
        """Method to extract, load and preprocess article metadata provided
        by Web of Science with support for multiprocessing.

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

        Returns
        -------
        assets : list(Asset)

        """
        start = timeit.default_timer()
        stream_processing_jobs = []

        for root, dirs, files in os.walk(data_dir):
            for name in files:
                file_path = data_dir + name
                stream_processing_job = {"preprocessing": preprocessing,
                                         "remove_stopwords": remove_stopwords,
                                         'file_path': file_path,
                                         'nodes_to_analyze': nodes_to_analyze,
                                         'stopwords': self.stop_words}
                stream_processing_jobs.append(stream_processing_job)

        p = Pool(processes=cpu_count() - 1)
        assets = p.map(stream_preprocessing, stream_processing_jobs)
        p.close()
        p.join()
        assets = list(itertools.chain.from_iterable(assets))

        # Logfile
        stop = timeit.default_timer()
        runtime = stop - start
        event_title = "Load and preprocess Academic Data from Directory"
        event_description = \
            "Importing " + str(len(assets)) + \
            " academic assets from directory into assetlist." \
            + " Preprocessing = " + str(preprocessing)
        append_logfile(logfile_path=self.logfile_path,
                       event_title=event_title,
                       event_description=event_description,
                       runtime=runtime)
        return assets
