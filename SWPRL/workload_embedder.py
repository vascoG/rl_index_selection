import logging

import gensim
from sklearn.decomposition import PCA

from index_selection_evaluation.selection.cost_evaluation import CostEvaluation
from index_selection_evaluation.selection.index import Index
from index_selection_evaluation.selection.workload import Query
from SWPRL.partition import Partition

from .boo import BagOfOperators


class WorkloadEmbedder(object):
    def __init__(self, query_texts, representation_size, database_connector, columns=None, retrieve_plans=False):
        self.STOPTOKENS = [
            "as",
            "and",
            "or",
            "min",
            "max",
            "avg",
            "join",
            "on",
            "substr",
            "between",
            "count",
            "sum",
            "case",
            "then",
            "when",
            "end",
            "else",
            "select",
            "from",
            "where",
            "by",
            "cast",
            "in",
        ]
        self.PARTITIONS_SIMULATED_IN_PARALLEL = 1000
        self.query_texts = query_texts
        self.representation_size = representation_size
        self.database_connector = database_connector
        self.plans = None
        self.columns = columns

        if retrieve_plans:
            cost_evaluation = CostEvaluation(self.database_connector)
            # [without partitions], [with partitions]
            self.plans = ([], [])
            for query_idx, query_texts_per_query_class in enumerate(query_texts):
                query_text = query_texts_per_query_class[0]
                query = Query(query_idx, query_text)
                plan = self.database_connector.get_plan(query)
                self.plans[0].append(plan)

            logging.critical(f"Creating all partitions of width 1.")

            created_partitions = 0
            while created_partitions < len(self.columns):
                potential_partitions = []
                for i in range(self.PARTITIONS_SIMULATED_IN_PARALLEL):
                    potential_partition = Partition(self.columns[created_partitions])
                    cost_evaluation.what_if.simulate_index(potential_partition, True) # TODO: simulate partitions
                    potential_partitions.append(potential_partition)
                    created_partitions += 1
                    if created_partitions == len(self.columns):
                        break

                for query_idx, query_texts_per_query_class in enumerate(query_texts):
                    query_text = query_texts_per_query_class[0]
                    query = Query(query_idx, query_text)
                    plan = self.database_connector.get_plan(query)
                    self.plans[1].append(plan)

                for potential_partition in potential_partitions:
                    cost_evaluation.what_if.drop_simulated_index(potential_index) # TODO: drop simulated partitions

                logging.critical(f"Finished checking {created_partitions} partitions.")

        self.database_connector = None

    def get_embeddings(self, workload):
        raise NotImplementedError


class PlanEmbedder(WorkloadEmbedder):
    def __init__(self, query_texts, representation_size, database_connector, columns, without_partitions=False):
        WorkloadEmbedder.__init__(
            self, query_texts, representation_size, database_connector, columns, retrieve_plans=True
        )

        self.plan_embedding_cache = {}

        self.relevant_operators = []
        self.relevant_operators_wo_partitions = []
        self.relevant_operators_with_partitions = []

        self.boo_creator = BagOfOperators()

        for plan in self.plans[0]:
            boo = self.boo_creator.boo_from_plan(plan)
            self.relevant_operators.append(boo)
            self.relevant_operators_wo_partitions.append(boo)

        if without_partitions is False:
            for plan in self.plans[1]:
                boo = self.boo_creator.boo_from_plan(plan)
                self.relevant_operators.append(boo)
                self.relevant_operators_with_partitions.append(boo)

        # Deleting the plans to avoid costly copying later.
        self.plans = None

        self.dictionary = gensim.corpora.Dictionary(self.relevant_operators)
        logging.warning(f"Dictionary has {len(self.dictionary)} entries.")
        self.bow_corpus = [self.dictionary.doc2bow(query) for query in self.relevant_operators]

        self._create_model()

        # Deleting the bow_corpus to avoid costly copying later.
        self.bow_corpus = None

    def _create_model(self):
        raise NotImplementedError

    def _infer(self, bow, boo):
        raise NotImplementedError

    def get_embeddings(self, plans):
        embeddings = []

        for plan in plans:
            cache_key = str(plan)
            if cache_key not in self.plan_embedding_cache:
                boo = self.boo_creator.boo_from_plan(plan)
                bow = self.dictionary.doc2bow(boo)

                vector = self._infer(bow, boo)

                self.plan_embedding_cache[cache_key] = vector
            else:
                vector = self.plan_embedding_cache[cache_key]

            embeddings.append(vector)

        return embeddings

class PlanEmbedderLSIBOW(PlanEmbedder):
    def __init__(self, query_texts, representation_size, database_connector, columns, without_partitions=False):
        PlanEmbedder.__init__(self, query_texts, representation_size, database_connector, columns, without_partitions)

    def _create_model(self):
        self.lsi_bow = gensim.models.LsiModel(
            self.bow_corpus, id2word=self.dictionary, num_topics=self.representation_size
        )

        assert (
            len(self.lsi_bow.get_topics()) == self.representation_size
        ), f"Topic-representation_size mismatch: {len(self.lsi_bow.get_topics())} vs {self.representation_size}"

    def _infer(self, bow, boo):
        result = self.lsi_bow[bow]

        if len(result) == self.representation_size:
            vector = [x[1] for x in result]
        else:
            vector = [0] * self.representation_size
            for topic, value in result:
                vector[topic] = value
        assert len(vector) == self.representation_size

        return vector
