"""This module defines the classes Asset and derived specialized academic / patent asset classes
and their associated methods.

"""

# standard library imports
import collections
import pickle
import operator
import functools as ft
import timeit

# related third party imports
# None

# local application/library specific imports
from logfile import append_logfile


class Asset:
    def __init__(self, year):
        self.file_directory = self.node_names = None
        self.year = year

    def asset_type(self):
        return "Asset"

    def matches_any_node(self):
        """check if the asset matches any node
        Returns
        -------
        true if asset matches any node
        """
        return self.node_names is not None and len(self.node_names) > 0

    def words_to_analyze(self):
        """virtual method to retrieve the words that have to be analyzed"""
        return []

    def find_nodes(self, nodes):
        """Associate node names to the asset
        If any synonym of a node is found in the asset, the name of the node is added to the node_names list.

        Parameters
        ----------
        nodes: Nodes

        Returns
        -------
        None
        """
        found_node_words = {}
        for word in self.words_to_analyze():
            found_nodes = nodes.get_nodes_by_synonym(word)
            if found_nodes is not None:
                for node in found_nodes:
                    if node not in found_node_words:
                        found_node_words[node] = {}
                    node_words = found_node_words[node]
                    if word not in node_words:
                        node_words[word] = 0
        # evaluate boolean conditions
        self.node_names = []
        for node in found_node_words.keys():
            if node.node_matches_found_synonyms(found_node_words[node]):
                self.node_names.append(node.name)


class AssetWords(Asset):
    def __init__(self, year, asset):
        Asset.__init__(self, year)
        self.words = asset.words_to_analyze()
        self.node_names = asset.node_names

    def asset_type(self):
        return "AssetWords"

    def words_to_analyze(self):
        return self.words


class SentenceLemmatizedAcademicAsset(Asset):
    def __init__(self, nodes, year, authors, title_sentences, abstract_sentences, keywords):
        Asset.__init__(self, year)
        self.title_sentences = title_sentences
        self.abstract_sentences = abstract_sentences
        self.keywords = keywords
        self.authors = authors
        self.find_nodes(nodes)

    def asset_type(self):
        return "AcademicAsset"

    def words_to_analyze(self):
        sentences = self.title_sentences + self.abstract_sentences
        return ft.reduce(operator.add, sentences)


class WordTokenizedAcademicAsset(Asset):
    def __init__(self, nodes, year, authors, tokenized_title, tokenized_abstract, keywords):
        Asset.__init__(self, year)
        self.tokenized_title = tokenized_title
        self.tokenized_abstract = tokenized_abstract
        self.keywords = keywords
        self.authors = authors
        self.find_nodes(nodes)

    def asset_type(self):
        return "AcademicAsset"

    def words_to_analyze(self):
        words = self.tokenized_title + self.tokenized_abstract
        return words


class PosTaggedAcademicAsset(Asset):
    def __init__(self, year, nodes, authors, pos_tagged_title, pos_tagged_abstract, keywords):
        Asset.__init__(self, year)
        self.pos_tagged_title = pos_tagged_title
        self.pos_tagged_abstract = pos_tagged_abstract
        self.keywords = keywords
        self.authors = authors
        self.find_nodes(nodes)

    def asset_type(self):
        return "AcademicAsset"

    def words_to_analyze(self):
        words = self.pos_tagged_title + self.pos_tagged_abstract
        return words


class LemmatizedAcademicAsset(Asset):
    def __init__(self, year, nodes, authors, lemmatized_title, lemmatized_abstract, keywords):
        Asset.__init__(self, year)
        self.lemmatized_title = lemmatized_title
        self.lemmatized_abstract = lemmatized_abstract
        self.keywords = keywords
        self.authors = authors
        self.find_nodes(nodes)

    def asset_type(self):
        return "AcademicAsset"

    def words_to_analyze(self):
        words = self.lemmatized_title + self.lemmatized_abstract
        return words


class SentenceLemmatizedPatentAsset(Asset):
    def __init__(self, year, nodes, assignees, title_sentences, abstract_sentences,
                 claims_sentences, description_sentences, cpc, ipc):
        Asset.__init__(self, year)
        self.title_sentences = title_sentences
        self.abstract_sentences = abstract_sentences
        self.claims_sentences = claims_sentences
        self.description_sentences = description_sentences
        self.assignees = assignees
        self.cpc = cpc
        self.ipc = ipc
        self.find_nodes(nodes)

    def asset_type(self):
        return "PatentAsset"

    def sentences_to_analyze(self):
        sentences = self.title_sentences + self.abstract_sentences + \
                    self.claims_sentences + self.description_sentences
        return sentences

    def words_to_analyze(self):
        return ft.reduce(operator.add, self.sentences_to_analyze())


class WordTokenizedPatentAsset(Asset):
    def __init__(self, year, nodes, assignees, tokenized_title, tokenized_abstract,
                 tokenized_claims, tokenized_description, cpc, ipc):
        Asset.__init__(self, year)
        self.tokenized_title = tokenized_title
        self.tokenized_abstract = tokenized_abstract
        self.tokenized_claims = tokenized_claims
        self.tokenized_description = tokenized_description
        self.assignees = assignees
        self.cpc = cpc
        self.ipc = ipc
        self.find_nodes(nodes)

    def asset_type(self):
        return "PatentAsset"

    def words_to_analyze(self):
        words = self.tokenized_title + self.tokenized_abstract + \
                self.tokenized_claims + self.tokenized_description
        return words


class PosTaggedPatentAsset(Asset):
    def __init__(self, year, nodes, assignees, pos_tagged_title, pos_tagged_abstract,
                 pos_tagged_claims, pos_tagged_description, cpc, ipc):
        Asset.__init__(self, year)
        self.pos_tagged_title = pos_tagged_title
        self.pos_tagged_abstract = pos_tagged_abstract
        self.pos_tagged_claims = pos_tagged_claims
        self.pos_tagged_description = pos_tagged_description
        self.assignees = assignees
        self.cpc = cpc
        self.ipc = ipc
        self.find_nodes(nodes)

    def asset_type(self):
        return "PatentAsset"

    def words_to_analyze(self):
        words = self.pos_tagged_title + self.pos_tagged_abstract + \
                self.pos_tagged_claims + self.pos_tagged_description
        return words


class LemmatizedPatentAsset(Asset):
    def __init__(self, year, nodes, assignees, lemmatized_title, lemmatized_abstract,
                 lemmatized_claims, lemmatized_description, cpc, ipc):
        Asset.__init__(self, year)
        self.lemmatized_title = lemmatized_title
        self.lemmatized_abstract = lemmatized_abstract
        self.lemmatized_claims = lemmatized_claims
        self.lemmatized_description = lemmatized_description
        self.assignees = assignees
        self.cpc = cpc
        self.ipc = ipc
        self.find_nodes(nodes)

    def asset_type(self):
        return "PatentAsset"

    def words_to_analyze(self):
        words = self.lemmatized_title + self.lemmatized_abstract + \
                self.lemmatized_claims + self.lemmatized_description
        return words


def save_assetlist_to_dir(assetlist, data_dir, filename):
    file_path = data_dir + filename
    with open(file_path, "wb") as fp:
        pickle.dump(assetlist, fp)
    print(str(len(assetlist)) + ' Assets saved to ' + file_path)


def load_assetlist_from_dir(file_path, logfile_path=None):
    start_time = timeit.default_timer()
    with open(file_path, "rb") as fp:
        assetlist = pickle.load(fp)
    event_description = str(len(assetlist)) + ' Assets loaded from ' + file_path
    stop_time = timeit.default_timer()
    runtime = stop_time - start_time
    event_title = 'Load list of assets'
    if logfile_path is not None:
        append_logfile(logfile_path=logfile_path,
                       event_title=event_title,
                       event_description=event_description,
                       runtime=runtime)

    return assetlist


def filter_assetlist_by_start_year(assetlist, year):
    newlist = []
    for asset in assetlist:
        if int(asset.year) >= year:
            newlist.append(asset)
    return newlist


class AssetWordIterator(collections.Iterator):
    def __init__(self, assetlist):
        self.index = -1
        self.assetlist = assetlist

    def __iter__(self):
        return self

    def next(self):
        if self.index < len(self.assetlist)-1:
            self.index += 1
            return self.assetlist[self.index].words_to_analyze()
        else:
            raise StopIteration
    __next__ = next
