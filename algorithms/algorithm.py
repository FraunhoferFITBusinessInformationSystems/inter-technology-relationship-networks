"""This module defines the base classes for all edge and node algorithms

"""

# standard library imports
import timeit

# related third party imports
from sklearn.metrics.pairwise import cosine_similarity

# local application/library specific imports
from logfile import append_logfile


tokenize = lambda doc: doc
""" dummy tokenizer
    because all tokenizing has been done during preprocessing, we need no tokenizer within algorithms
"""


class Algorithm:
    def __init__(self, alg_name='undefined', cumulative=True, logfile_path=None):
        if cumulative:
            self.alg_name = 'c_' + alg_name
        else:
            self.alg_name = alg_name

        self.cumulative = cumulative
        self.logfile_path = logfile_path
        self.start_time = None

    def run(self, nodes, results, year=0):
        """virtual method to run the algorithm"""
        raise NotImplementedError()

    @staticmethod
    def calc_similarity(vector1, vector2):
        result = cosine_similarity(vector1, vector2)
        return result[0][0]

    def get_documents_from_nodelist(self, node_list, year):
        """ get a list of words for all assets belonging to each node in the node_list

        Parameters
        ----------
        node_list : list of Node
            Specifies the nodes to be analyzed (input parameter).
        year : integer
            the year of the assets that have to be analyzed. If the cumulative parameter of the algorithm is true
            all assets from years before are also included

        Returns
        ----------
        all_documents : Array of List of Words

        """
        all_documents = []
        for node in node_list:
            if self.cumulative:
                words = node.get_words_cumulative(year)
            else:
                words = node.get_words(year)
            all_documents.append(words)
        return all_documents

    def calc_document_similarity(self, year, nodelist, document_term_matrix, results):
        results.begin_bulk_insert()
        for count_0, doc_0 in enumerate(document_term_matrix):
            for count_1, doc_1 in enumerate(document_term_matrix):
                if count_1 > count_0:
                    if doc_0.getnnz() > 0 and doc_1.getnnz() > 0:
                        results.add_edge_value(year, nodelist[count_0], self.alg_name, nodelist[count_1],
                                               self.calc_similarity(doc_0, doc_1))
                    else:
                        if count_0 < len(nodelist) and count_1 < len(nodelist):
                            results.add_edge_value(year, nodelist[count_0], self.alg_name, nodelist[count_1], 0)
                        else:
                            print("Error: illegal count")
        results.end_bulk_insert()

    def start_timer(self):
        """Start timer for logging"""
        self.start_time = timeit.default_timer()

    def stop_timer_and_log(self, details=''):
        """Log to logfile"""
        stop_time = timeit.default_timer()
        runtime = stop_time - self.start_time
        event_title = 'Algorithm: ' + self.alg_name
        event_description = details
        if self.logfile_path is not None:
            append_logfile(logfile_path=self.logfile_path,
                           event_title=event_title,
                           event_description=event_description,
                           runtime=runtime)
