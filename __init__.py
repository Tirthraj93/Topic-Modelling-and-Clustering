import re, time, lda, numpy as np

from FileUtil import *
from Constants import *
from ElasticSearchUtil import ElasticSearchUtility
from datasets import *


TITLES_SIZE = None
TOKENS_SIZE = None

# as doc-terms sparse features are common, we are having it stored in a global dictionary,
# so that it need not be created again
DOC_TERMS_DICT = dict()


def store_lda_datafiles(q_id_list):
    """
    Fetch and store data as per lda data set requirements for all queries

    :param q_id_list: list of queries to process
    """

    # for all queries
        # get set of top 1000 bm25 docs for query
        # get set of qrel docs for query
        # union both the sets
        # store ldac
        # store tokens
        # store titles

    results_query_docs_dict = get_query_docs_from_results(RESULTS_FILE)
    qrel_query_docs_dict = get_query_docs_from_qrel(q_id_list, QREL_FILE)
    # docno_id_dict = es_util.get_ids_for_field(AP_89_INDEX, AP_89_DOC_TYPE, "docno")

    for q_id in q_id_list:

        print 'Processing - ', q_id

        bm25_docs = set(results_query_docs_dict[q_id])
        qrel_docs = set(qrel_query_docs_dict[q_id])

        docs_set = bm25_docs.union(qrel_docs)

        query_file_prefix = join(QUERY_DATA_PATH, q_id)
        ldac_file = query_file_prefix + ".ldac"
        tokens_file = query_file_prefix + ".tokens"
        titles_file = query_file_prefix + ".titles"

        store_lda_data_for_docs(docs_set, ldac_file, tokens_file, titles_file)


def store_lda_data_for_docs(docs_set, ldac_file, tokens_file, titles_file, write_count=None, cache=True):
    """
    Transform contents of given document list into ldac, tokens and titles;
    and store in their respective files as given.


    :param docs_set: set to transform data of
    :param ldac_file: ldac data file
    :param tokens_file: tokens data file
    :param titles_file: titles data file
    :param write_count: count after which write_list is to be dumped
    :param cache: If true, maintains feature string for each doc processed
    """
    write_list_ldac = []
    write_list_tokens = []
    write_list_titles = []

    doc_index = 0
    term_index = 0
    term_indices = dict()

    es_util = ElasticSearchUtility()

    for doc in docs_set:

        # print '\t', doc_index

        # update titles
        write_list_titles.append(str(doc_index) + ' ' + doc)

        sparse_features = None

        if doc not in DOC_TERMS_DICT:
            # doc_id = docno_id_dict[doc]
            sparse_features = es_util.get_sparse_tf_features(AP_89_INDEX,
                                                             AP_89_DOC_TYPE,
                                                             "text",
                                                             doc)
            if cache:
                DOC_TERMS_DICT[doc] = sparse_features
        else:
            sparse_features = DOC_TERMS_DICT[doc]


        # get total unique features
        n_unique_features = str(len(sparse_features))

        if n_unique_features != '0':
            write_features = dict()
            for feature in sparse_features:
                # update tokens
                if feature not in term_indices:
                    write_list_tokens.append(feature)
                    term_indices[feature] = term_index
                    term_index += 1

                int_feature_index = term_indices[feature]
                str_feature_index = str(int_feature_index)
                feature_value = str(sparse_features[feature])
                write_features[int_feature_index] = ' ' + str_feature_index + ':' + feature_value

            # sort features in ascending order
            write_features = sorted(write_features.items(), key=lambda x: x[0])

            # append doc line to write list
            write_list_ldac.append(n_unique_features + ''.join(x[1] for x in write_features))
        else:
            write_list_ldac.append('')

        doc_index += 1

        if write_count is not None and (doc_index % write_count) == 0:
            print '\t', doc_index
            append_list_to_file(write_list_ldac, ldac_file)
            write_list_ldac = []

            append_list_to_file(write_list_tokens, tokens_file)
            write_list_tokens = []

            append_list_to_file(write_list_titles, titles_file)
            write_list_titles = []

    append_list_to_file(write_list_ldac, ldac_file)
    append_list_to_file(write_list_tokens, tokens_file)
    append_list_to_file(write_list_titles, titles_file)


def perform_lda(output_file, n_topics, n_iter, top_words_count, query=None):
    """
    Perform LDA on given query and store results in given output file

    :param query: query to process LDA for
    :param output_file: file to store LDA results in
    :param n_topics: number of topics to discover using LDA
    :param n_iter: iterations for LDA
    :param top_words_count: total words to get for each topic
    """

    write_list = []

    if query is None:
        info = 'Processing all docs...'
        path_prefix = join(DATA_PATH, "all_docs")
    else:
        info = 'Query: ' + query
        path_prefix = join(QUERY_DATA_PATH, query)
        write_list.append(info)

    print info

    print '\tLoading ldac...'
    x = load_ldac(path_prefix + ".ldac")
    print '\tLoading tokens...'
    vocab = load_vocab(path_prefix + ".tokens")
    print '\tLoading titles...'
    titles = load_titles(path_prefix + ".titles")

    model = lda.LDA(n_topics=n_topics, n_iter=n_iter, random_state=1)
    model.fit(x)
    topic_word = model.topic_word_
    top_words_count += 1

    for i, topic_dist in enumerate(topic_word):
        topic_words = np.array(vocab)[np.argsort(topic_dist)][:-top_words_count:-1]
        # flatten words' list to string
        topic_details = '\tTopic {}: {}'.format(i, ' '.join(topic_words))
        write_list.append(topic_details)
        # print(topic_details)

    doc_topic = model.doc_topic_

    for i, title in enumerate(titles):
        doc_topics = ' '.join(str(no) for no in doc_topic[i])
        document_details = "\t{}\n\t\t{}".format(title, doc_topics)
        write_list.append(document_details)
        # print(document_details)

    append_list_to_file(write_list, output_file)


def get_attributes(f_key):
    """


    :param f_key:
    """

    attributes_list = []

    attribute_string = "@attribute "
    type_string = " numeric"

    with open(f_key, 'r') as f:
        lines = f.read().splitlines()
        for line in lines:
            words_array = re.split('\\s+', line.strip())
            attribute = words_array[0]
            attributes_list.append(attribute_string + attribute + type_string)

    return attributes_list


def create_arff(f_composition, f_keys, f_arff, f_docs):
    """


    :param f_composition:
    :param f_keys:
    :param f_docs:
    """
    remove_if_exists(f_arff)
    remove_if_exists(f_docs)

    arff_write_list = []

    comment1 = "%% AP89 Topic Modelling Data for Clustering"
    comment2 = "%% Author: Tirthraj\n"
    relation = "@relation docTopicDist"
    attributes_list = get_attributes(f_keys)

    arff_write_list.append(comment1)
    arff_write_list.append(comment2)
    arff_write_list.append(relation)
    arff_write_list += attributes_list

    append_list_to_file(arff_write_list, f_arff)

    arff_write_list = ["\n@data"]
    doc_write_list = []

    count = 0

    with open(f_composition, 'r') as f:
        lines = f.read().splitlines()

        for line in lines:
            words_array = re.split('\\s+', line.strip())
            file_string = words_array[1]
            doc_string = re.split('/', file_string)[3]
            value_string = ','.join('%f' % float(value) for value in words_array[2:])

            doc_write_list.append(doc_string)
            arff_write_list.append(value_string)

            count += 1

            if count % 1000 == 0:
                print 'Wrote ', count
                append_list_to_file(arff_write_list, f_arff)
                append_list_to_file(doc_write_list, f_docs)
                arff_write_list = []
                doc_write_list = []

        append_list_to_file(arff_write_list, f_arff)
        append_list_to_file(doc_write_list, f_docs)


if __name__ == '__main__':

    ##################################
    #  Topic Models per query
    ##################################
    query_id_list = read_word_into_list(QUERIES_FILE)
    ##################################
    # start_time = time.time()
    # remove_create_directory(QUERY_DATA_PATH)
    # store_lda_datafiles(query_id_list)
    # end_time = time.time()
    # print 'Execution Time: ', (end_time - start_time) / 60
    ##################################
    # start_time = time.time()
    #
    # remove_if_exists(PART_A_OUTPUT_FILE)
    #
    # for query_id in query_id_list:
    #     perform_lda(PART_A_OUTPUT_FILE, 20, 25, 30, query=query_id)
    #
    # end_time = time.time()
    # print 'Execution Time: ', (end_time - start_time) / 60

    ##################################
    #  LDA-topics and clustering
    ##################################
    # start_time = time.time()
    #
    # if exists(ALL_IDS_FILE):
    #     doc_set = read_word_into_list(ALL_IDS_FILE)
    # else:
    #     es_utility = ElasticSearchUtility()
    #     doc_set = es_utility.get_all_ids(AP_89_INDEX, AP_89_DOC_TYPE)
    #     write_list_to_file(doc_set, ALL_IDS_FILE)
    #
    # file_prefix = join(DATA_PATH, "all_docs")
    # f_ldac = file_prefix + ".ldac"
    # f_tokens = file_prefix + ".tokens"
    # f_titles = file_prefix + ".titles"
    #
    # remove_if_exists(f_ldac)
    # remove_if_exists(f_tokens)
    # remove_if_exists(f_titles)
    #
    # store_lda_data_for_docs(doc_set, f_ldac, f_tokens, f_titles,
    #                         write_count=1000, cache=False)
    #
    # end_time = time.time()
    # print 'Execution Time: ', (end_time - start_time) / 60
    ##################################
    # start_time = time.time()
    #
    # remove_if_exists(PART_B_OUTPUT_FILE)
    # perform_lda(PART_B_OUTPUT_FILE, 200, 25, 30)
    #
    # end_time = time.time()
    # print 'Execution Time: ', (end_time - start_time) / 60
    ##################################
    # es_utility = ElasticSearchUtility()
    # # remove_create_directory(AP_DATA_PATH)
    # es_utility.store_document(AP_89_INDEX, AP_89_DOC_TYPE, "text")

    # create_arff(COMPOSITIONS_FILE, KEYS_FILE, ARFF_FILE, DOCS_FILE)
    ##################################
    doc_cluster_dict = get_doc_cluster_dict(CLUSTERING_OUTPUT_FILE)
    qdocs_tuple_list = get_query_doc_tuple(query_id_list, QREL_FILE)

    # TRUE POSITIVE
    same_query_same_cluster = 0.0
    # FALSE NEGATIVE
    same_query_diff_cluster = 0.0
    # FALSE POSITIVE
    diff_query_same_cluster = 0.0
    # TRUE NEGATIVE
    diff_query_diff_cluster = 0.0

    length = len(qdocs_tuple_list)

    for i in range(0, length):
        for j in range(i, length):
            if i != j:
                query1 = qdocs_tuple_list[i][0]
                query2 = qdocs_tuple_list[j][0]

                document1 = qdocs_tuple_list[i][1]
                document2 = qdocs_tuple_list[j][1]

                cluster1 = doc_cluster_dict[document1]
                cluster2 = doc_cluster_dict[document2]

                if query1 == query2:
                    if cluster1 == cluster2:
                        same_query_same_cluster += 1.0
                    else:
                        same_query_diff_cluster += 1.0
                else:
                    if cluster1 == cluster2:
                        diff_query_same_cluster += 1.0
                    else:
                        diff_query_diff_cluster += 1.0

    # precision_numerator = TP + TN
    precision_num = same_query_same_cluster + diff_query_diff_cluster
    # precision_denominator = TP + TN + FP + FN
    precision_den = precision_num + same_query_diff_cluster + diff_query_same_cluster

    precision = precision_num / precision_den

    print "same_query_same_cluster - ", str(same_query_same_cluster)
    print "same_query_diff_cluster - ", str(same_query_diff_cluster)
    print "diff_query_same_cluster - ", str(diff_query_same_cluster)
    print "diff_query_diff_cluster - ", str(diff_query_diff_cluster)
    print "PRECISION: ", precision
