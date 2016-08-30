from elasticsearch import Elasticsearch
from os.path import join

from Constants import ES_HOST, AP_DATA_PATH
from FileUtil import write_list_to_file


class ElasticSearchUtility:
    """
    class to communicate with elasticsearch
    """

    def __init__(self):
        self.es = Elasticsearch(hosts=[ES_HOST], timeout=750)

    def get_sparse_tf_features(self, index, doc_type, field, doc_id, terms_to_include=None):
        """
        Get an dictionary of format {term1:tf1, term2:tf2, ...} for all terms in given field of given index's doc_id
        where tf id greater than 0

        :param index: name of the index
        :param doc_type: type of the document
        :param field: field to get terms of
        :param doc_id: index document id
        :return: dictionary of the format {term1:tf1, term2:tf2, ...} for all terms having tf > 0
        """

        out_dict = dict()

        # POST trec_spam/documents/1/_termvector?field_statistics=false
        # &positions=false&offsets=false
        # &fields=body_shingles
        response = self.es.termvectors(index, doc_type, doc_id, fields=[field],
                                       field_statistics=False, positions=False, offsets=False)

        try:
            terms = response["term_vectors"][field]["terms"]
        except KeyError:
            return dict()

        for term in terms:
            tf = terms[term]["term_freq"]

            if tf > 0:
                try:
                    decoded_term = str(term)

                    if terms_to_include is None:
                        out_dict[decoded_term] = tf
                    else:
                        if terms_to_include.__contains__(decoded_term):
                            out_dict[decoded_term] = tf
                except:
                    pass

        return out_dict

    def get_all_ids(self, index, doc_type):
        """
        Retrieve set of document_id given index-doc_type

        :param index: name of the index
        :param doc_type: type of the document
        :return: set of all doc_id
        """

        print 'Retrieving all documents'

        docs_set = set()

        query = {
            "query": {
                "match_all": {}
            }
        }

        # query scroll
        scroll = self.es.search(
            index=index,
            doc_type=doc_type,
            scroll='10m',
            size=1000,
            body=query,
            fields=[])

        # set initial scroll size
        scroll_size = scroll['hits']['total']

        # retrieve results
        size = 0
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']

            for hit in hits_list:
                doc_id = hit["_id"]
                docs_set.add(doc_id)

            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled - ", str(size)
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')

        return docs_set

    def store_document(self, index, doc_type, field):
        """
        Store value of given field in a file with doc id as filename

        :param index: name of the index
        :param doc_type: type of the document
        :param field: name of the field
        """
        query = {
            "query": {
                "match_all": {}
            }
        }

        # query scroll
        scroll = self.es.search(
            index=index,
            doc_type=doc_type,
            scroll='10m',
            size=1000,
            body=query,
            fields=[field])

        # set initial scroll size
        scroll_size = scroll['hits']['total']

        # retrieve results
        size = 0
        while scroll_size > 0:
            # scrolled data is in scroll['hits']['hits']
            hits_list = scroll['hits']['hits']

            for hit in hits_list:
                doc_id = hit["_id"]
                field_value = str(hit["fields"][field][0])

                write_list = [field_value]
                write_list_to_file(write_list, join(AP_DATA_PATH, doc_id),
                                   print_progress=False)

            # update scroll size
            scroll_size = len(scroll['hits']['hits'])
            size += scroll_size
            print "scrolled - ", str(size)
            # prepare next scroll
            scroll_id = scroll['_scroll_id']
            # perform next scroll
            scroll = self.es.scroll(scroll_id=scroll_id, scroll='10m')
