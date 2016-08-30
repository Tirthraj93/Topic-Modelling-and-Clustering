import re
from shutil import rmtree
from os import remove, mkdir
from os.path import basename


def read_word_into_list(read_file):
    """
    Reads each line into file in an array where each elements

    :param read_file: file to read
    :return: the list read from file
    """

    data_array = []

    print 'Reading - ', basename(read_file)

    with open(read_file, 'r') as in_file:
        lines = in_file.read().splitlines()
        for line in lines:
            data_array.append(line.strip())

    return data_array


def get_query_docs_from_results(results_file):
    """
    Get a dictionary of set of top documents in descending order for each query from results file,
    as {query_id:(doc1, doc2, ...)}

    :param results_file: file containing top docs for queries
    :return: a dictionary of format {query_id:(doc1, doc2, ...)}
    """

    print 'Getting qdocs from results...'

    out_dict = dict()

    with open(results_file, 'r') as in_file:
        lines = in_file.read().splitlines()
        for line in lines:
            words_array = re.split('\\s+', line.strip())
            query_id = words_array[0]
            doc_id = words_array[2]

            if query_id not in out_dict:
                out_dict[query_id] = [doc_id]
            else:
                out_dict[query_id].append(doc_id)

    return out_dict


def get_query_docs_from_qrel(query_id_list, qrel_file):
    """
        Get a dictionary of documents for each query from qrel file,
        as {query_id:[doc1, doc2, ...]}

        :param query_id_list: list of query ids for which qrel docs are to be fetched
        :param qrel_file: file containing top docs for queries
        :return: a dictionary of format {query_id:[doc1, doc2, ...]}
    """

    print 'Getting qdocs from qrel...'

    out_dict = dict()

    with open(qrel_file, 'r') as in_file:
        lines = in_file.read().splitlines()
        for line in lines:
            words_array = re.split('\\s+', line.strip())
            query_id = words_array[0]
            doc_id = words_array[2]
            relevance = words_array[3]

            if relevance == '1' and query_id_list.__contains__(query_id):
                if query_id not in out_dict:
                    out_dict[query_id] = [doc_id]
                else:
                    out_dict[query_id].append(doc_id)

    return out_dict


def write_list_to_file(write_list, write_file, print_progress=True):
    """
    Writes given list to a file as each list element in one line

    :param write_list: list to write
    :param write_file: destination file
    :param print_progress: prints progress if True
    """

    if print_progress:
        print '\twriting to ', basename(write_file)

    with open(write_file, 'w') as out_file:
        for element in write_list:
            out_line = "{}\n".format(element)
            out_file.write(out_line)


def append_list_to_file(write_list, write_file):
    """
    Appends given list to a file as each list element in one line

    :param write_list: list to write
    :param write_file: destination file
    """

    print '\tappending to ', basename(write_file)

    with open(write_file, 'a+') as out_file:
        for element in write_list:
            out_line = "{}\n".format(element)
            out_file.write(out_line)


def remove_if_exists(file_path):
    """
    Remove given file if it exists

    :param file_path: file to remove
    :return:
    """
    try:
        print 'Deleting ', basename(file_path)
        remove(file_path)
    except WindowsError:
        pass


def remove_create_directory(directory):
    """
    Removes existing directory and creates one

    :param directory: directory to remove and create
    """
    try:
        print 'Removing ', basename(directory)
        rmtree(directory)
    except WindowsError:
        pass

    print 'Creating ', basename(directory)
    mkdir(directory)


def get_query_doc_tuple(query_list, qrel_file):
    """
    Get tuple of (query, doc) for all relevant documents of given query list from qrel file

    :param query_list: list of queries tp get documents of
    :param qrel_file: qrel file
    :return: list of (query, document) tuple
    """

    print 'Getting list of query-docs tuple from qrel...'

    out_list = []

    with open(qrel_file, 'r') as in_file:
        lines = in_file.read().splitlines()
        for line in lines:
            words_array = re.split('\\s+', line.strip())
            query_id = words_array[0]
            doc_id = words_array[2]
            relevance = words_array[3]

            if relevance == '1' and query_list.__contains__(query_id):
                t = (query_id, doc_id)
                out_list.append(t)

    return out_list


def get_doc_cluster_dict(cl_output_flie):
    """
    Reads clustering output file into a dictionary of format {document:cluster}

    :param cl_output_flie: clustering output file
    :return: ictionary of format {document:cluster}
    """

    print "Reading ", basename(cl_output_flie)

    out_dict = dict()

    with open(cl_output_flie, 'r') as in_file:
        lines = in_file.read().splitlines()
        for line in lines:
            words_array = re.split('\\s+', line.strip())
            document = words_array[0]
            cluster = words_array[1].split('.')[0]
            out_dict[document] = cluster

    return out_dict
