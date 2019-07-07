"""This module defines the processing pipeline.

"""

# standard library imports
import nltk

# related third party imports
# None

# local application/library specific imports
from logfile import create_logfile, pretty_print_logfile, end_logfile
from networks import *
from nodes import *
from assets import *
from algorithms.bag_of_words_algorithms import *
from algorithms.node_cooccurrence import *
from algorithms.doc2vec_algorithms import *
from algorithms.node_algorithms import *
from results import *
from academic_data_acquisition import AcademicData
from patent_data_acquisition import PatentData

if __name__ == '__main__':  # prevent execution in parallel processes

    # =============================================================================
    # Post Setup
    # =============================================================================

    nltk.download('averaged_perceptron_tagger')
    nltk.download('wordnet')
    nltk.download('punkt')
    nltk.download('stopwords')

    # =============================================================================
    # Preparing Data Analysis
    # =============================================================================

    # Set Directories
    DATA_DIR = "F:/github_example/"
    DATA_DIR_PARSED_PATENTS = DATA_DIR + "/parsed_patents/" # directory of parsed patent files
    RESULT_DIR = DATA_DIR + "Results/"
    ASSETLIST_DIR = DATA_DIR + "Assets/"
    LOGFILE_PATH = DATA_DIR + "log.txt"

    PIPELINE_NAME = "github_example"

    LOGFILE = create_logfile(LOGFILE_PATH)  # Creates a new logfile with an initial entry

    # =============================================================================
    # Defining our technologies of interest and their synonyms (i.e., nodes)
    # =============================================================================
    NODES = Nodes(ASSETLIST_DIR)

    #Nodes.add_node(Node('Node's name', 'Search string')
    #Don't forget the apostrophes (') around the node's name and the search string

    NODES.add_node(Node('Active learning', '"Active Learning"'))
    NODES.add_node(Node('Activity classification', '"Activity classification"'))
    NODES.add_node(Node('Activity recognition', '"Activity recognition" OR "Activity detection"'))
    NODES.add_node(Node('AdaBoost', '"AdaBoost"'))
    NODES.add_node(Node('Affective computing','("emotions" OR "human affects" OR "affective") AND ("computing" OR "Artificial Intelligence" OR "Machine Learning")'))
    NODES.add_node(Node('Autonomous agents', '"Autonomous agent" OR "Autonomous agents"'))
    NODES.add_node(Node('Anomaly detection', '"Anomaly detection" OR (("Anomaly" OR "Anomalies") AND ("detecting" OR "detect"))'))
    NODES.add_node(Node('Artificial neural network', '"Artificial neural network" OR (("Neural network" OR "Neural net") AND ("learning" OR "training"))'))
    NODES.add_node(Node('Augmented reality', '"augmented reality" OR "augmented-reality"'))
    NODES.add_node(Node('Autoassociative neural network', '"Autoassociative neural network" OR "Autoassociative artificial neural network"'))
    NODES.add_node(Node('Autonomous robot', '"Autonomous" AND ("Robot" OR "Robots" OR "robotic")'))
    NODES.add_node(Node('Backpropagation', '"Backpropagation" OR "Backward propagation of error"'))
    NODES.add_node(Node('Bayesian network', '"Bayesian network" OR "Bayes network" OR "Belief network"'))

    # =============================================================================
    # Patent Data Acquisition and Preprocessing
    # =============================================================================

    PREPROCESS_PATENTS = True  # False if earlier preprocessed assets should be used

    if PREPROCESS_PATENTS:
        NODES.remove_assets()
        PatentData(logfile_path=LOGFILE_PATH). \
            preprocess_patent_files_from_dir(data_dir=DATA_DIR_PARSED_PATENTS,
                                             preprocessing="lemmatize",
                                             remove_stopwords="Nltk-Stopwords",
                                             nodes_to_analyze=NODES,
                                             filter_patents_by_node=True)
    else:
        NODES.read_assets()
        print("finished assets reading into nodes")

    # =============================================================================
    # Build list of all assets
    # =============================================================================

'''
    ASSETLIST = []
    ASSETLIST.extend(ASSETLIST_PATENT_2007)
    ASSETLIST.extend(ASSETLIST_PATENT_2008)
    ASSETLIST.extend(ASSETLIST_PATENT_2009)
    ASSETLIST.extend(ASSETLIST_PATENT_2010)
    ASSETLIST.extend(ASSETLIST_PATENT_2011)
    ASSETLIST.extend(ASSETLIST_PATENT_2012)
    ASSETLIST.extend(ASSETLIST_PATENT_2013)
    ASSETLIST.extend(ASSETLIST_PATENT_2014)
    ASSETLIST.extend(ASSETLIST_PATENT_2015)
    ASSETLIST.extend(ASSETLIST_PATENT_2016)
    ASSETLIST.extend(ASSETLIST_PATENT_2017)
    ASSETLIST.extend(ASSETLIST_PATENT_2018)

    NODES.enrich_with_assets(ASSETLIST)

    # =============================================================================
    # Run algorithms
    # =============================================================================

    RESULTS = Results()

    YEARS = [2018]  # years to be analyzed; You could add more years to the list

    # Edge algorithms
    bow_tfidf_alg = BagOfWords_Tfidf(logfile_path=LOGFILE_PATH)
    bow_tfidf_alg.run(asset_list=ASSETLIST, nodes=NODES, results=RESULTS, years=YEARS)

    d2w_alg = Doc2Vec(logfile_path=LOGFILE_PATH)
    d2w_alg.run(asset_list=ASSETLIST, nodes=NODES, results=RESULTS, years=YEARS)

    # NodeCoOccurrence
    node_cooc_alg = NodeCoOccurrence(logfile_path=LOGFILE_PATH)
    node_cooc_alg.run(asset_list=ASSETLIST, nodes=NODES, results=RESULTS, years=YEARS)

    # Node algorithms
    wia_alg = WordInAssetOccurrence(logfile_path=LOGFILE_PATH)
    wia_alg.run(asset_list=ASSETLIST, nodes=NODES, results=RESULTS, years=YEARS)

    # =============================================================================
    # Generate network graph from results
    # =============================================================================

    EDGE_ALGORITHMS_IN_GRAPH = [bow_alg.alg_name]
    Network().generate_multi_graph(nodelist=NODES.nodelist,
                                   results=RESULTS,
                                   years=YEARS,
                                   edge_algorithms=EDGE_ALGORITHMS_IN_GRAPH,
                                   data_dir=RESULT_DIR)

    # =============================================================================
    # Save results for post processing in excel
    # =============================================================================

    RESULTS.save_to_csv(RESULT_DIR + 'Results_' + PIPELINE_NAME +'.csv')

    # =============================================================================
    # Terminate pipeline
    # =============================================================================

    end_logfile(logfile_path=LOGFILE_PATH)
    pretty_print_logfile(logfile_path=LOGFILE_PATH)
'''

