"""This module defines the bag of words algorithms.

"""

# standard library imports
# None

# related third party imports
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.preprocessing import Normalizer

# local application/library specific imports
from algorithms.algorithm import *
from assets import get_years


class BagOfWords(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'BagOfWords', cumulative, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the Bag of Words Algorithm.

        Parameters
        ----------
        asset_list : list of Asset
        nodes : Nodes
            Specifies the nodes to be analyzed (input parameter).
        results : results.Results object
            Specifies the results object (output parameter) to which results should be added.
        years : list of integer
            if years is None all years from assets in the asset_list are evaluated.

        Returns
        ----------
        None

        """
        self.start_timer()
        if years is None:
            years = get_years(asset_list)
        nodelist = nodes.nodelist
        for year in years:
            all_documents = self.get_documents_from_nodelist(nodelist, year)

            vectorizer = CountVectorizer(min_df=1, lowercase=False, tokenizer=tokenize)
            document_term_matrix = vectorizer.fit_transform(all_documents)

            self.calc_document_similarity(year, nodelist, document_term_matrix, results)
        self.stop_timer_and_log()


# noinspection PyPep8Naming
class BagOfWords_Tfidf(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'BagOfWords_Tfidf', cumulative, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the Bag of Words Algorithm with evaluated term frequencies.

        Parameters
        ----------
        asset_list : list of Asset
        nodes : Nodes
            Specifies the nodes to be analyzed (input parameter).
        results : results.Results object
            Specifies the results object (output parameter) to which results should be added.
        years : list of integer
            if years is None all years from assets in the asset_list are evaluated.

        Returns
        ----------
        None

        """
        self.start_timer()
        if years is None:
            years = get_years(asset_list)
        nodelist = nodes.nodelist
        for year in years:
            all_documents = self.get_documents_from_nodelist(nodelist, year)

            vectorizer = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, smooth_idf=False, lowercase=False,
                                         sublinear_tf=True, tokenizer=tokenize)
            document_term_matrix = vectorizer.fit_transform(all_documents)

            self.calc_document_similarity(year, nodelist, document_term_matrix, results)
        self.stop_timer_and_log()


class SVD_BagOfWords(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'SVD_BagOfWords', cumulative, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the Bag of Words Algorithm.

        Parameters
        ----------
        asset_list : list of Asset
        nodes : Nodes
            Specifies the nodes to be analyzed (input parameter).
        results : results.Results object
            Specifies the results object (output parameter) to which results should be added.
        years : list of integer
            if years is None all years from assets in the asset_list are evaluated.

        Returns
        ----------
        None

        """
        self.start_timer()
        if years is None:
            years = get_years(asset_list)
        nodelist = nodes.nodelist
        if len(nodelist) < 200:
            dim = int(len(nodelist) / 2)
        else:
            dim = 100
        for year in years:
            all_documents = self.get_documents_from_nodelist(nodelist, year)

            vectorizer = CountVectorizer(min_df=1, lowercase=False, tokenizer=tokenize)
            dtm = vectorizer.fit_transform(all_documents)
            lsa = TruncatedSVD(dim, algorithm='randomized')  # arpack
            dtm_lsa = lsa.fit_transform(dtm)
            dtm_lsa = Normalizer(copy=False).fit_transform(dtm_lsa)

            results.begin_bulk_insert()
            for node_index in range(len(dtm_lsa)):
                for other_node_index in range(len(dtm_lsa)):
                    if other_node_index > node_index:
                        results.add_edge_value(year, nodelist[node_index], self.alg_name, nodelist[other_node_index],
                                               self.calc_similarity(dtm_lsa[node_index].reshape(1, -1),
                                                                    dtm_lsa[other_node_index].reshape(1, -1)))
            results.end_bulk_insert()
        self.stop_timer_and_log('dim: ' + str(dim))


class SVD_BagOfWords_Tfidf(Algorithm):
    def __init__(self, cumulative=True, logfile_path=None):
        Algorithm.__init__(self, 'SVD_BagOfWords_Tfidf', cumulative, logfile_path)

    def run(self, asset_list, nodes, results, years=None):
        """ Runs the Bag of Words Algorithm with evaluated term frequencies.

        Parameters
        ----------
        asset_list : list of Asset
        nodes : Nodes
            Specifies the nodes to be analyzed (input parameter).
        results : results.Results object
            Specifies the results object (output parameter) to which results should be added.
        years : list of integer
            if years is None all years from assets in the asset_list are evaluated.

        Returns
        ----------
        None

        """
        self.start_timer()
        if years is None:
            years = get_years(asset_list)
        nodelist = nodes.nodelist
        if len(nodelist) < 200:
            dim = int(len(nodelist) / 2)
        else:
            dim = 100
        for year in years:
            all_documents = self.get_documents_from_nodelist(nodelist, year)

            vectorizer = TfidfVectorizer(norm='l2', min_df=0, use_idf=True, smooth_idf=False, lowercase=False,
                                         sublinear_tf=True, tokenizer=tokenize)
            dtm = vectorizer.fit_transform(all_documents)
            lsa = TruncatedSVD(dim, algorithm='randomized')  # arpack
            dtm_lsa = lsa.fit_transform(dtm)
            dtm_lsa = Normalizer(copy=False).fit_transform(dtm_lsa)

            results.begin_bulk_insert()
            for node_index in range(len(dtm_lsa)):
                for other_node_index in range(len(dtm_lsa)):
                    if other_node_index > node_index:
                        results.add_edge_value(year, nodelist[node_index], self.alg_name, nodelist[other_node_index],
                                               self.calc_similarity(dtm_lsa[node_index].reshape(1, -1),
                                                                    dtm_lsa[other_node_index].reshape(1, -1)))
            results.end_bulk_insert()
        self.stop_timer_and_log('dim: ' + str(dim))
