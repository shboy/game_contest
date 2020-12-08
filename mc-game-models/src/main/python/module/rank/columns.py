import tensorflow as tf
from tensorflow import feature_column as fc
from rank.feature_table import *

from absl import flags, logging
FLAGS = flags.FLAGS

_WEIGHTED_SUFFIX = "_VALUE"


def _add_embedding_columns(columns, features, feature_table, vocabulary):
    for f in features:
        assert f in feature_table
        column = fc.embedding_column(fc.categorical_column_with_vocabulary_list(f, vocabulary.vocab[f]),
                                     feature_table[f].emb_width, combiner='sqrtn')
        columns.append(column)


def _add_weighted_embedding_columns(columns, features, feature_table, vocabulary):
    for f in features:
        assert f in feature_table
        weighted_column = fc.weighted_categorical_column(
            fc.categorical_column_with_vocabulary_list(f, vocabulary.vocab[f]), f + _WEIGHTED_SUFFIX)
        emb_weighted_column = fc.embedding_column(weighted_column, feature_table[f].emb_width, combiner='sqrtn')
        columns.append(emb_weighted_column)


def create_columns(feature_table, vocabulary, group=None):
    columns = []

    cate_features = [f for f in feature_table if feature_table[f].feature_spec.dtype == "string"]
    weighted_features = [f for f in feature_table if feature_table[f].feature_spec.dtype == "weighted"]

    # embedding_column
    _add_embedding_columns(columns, cate_features, feature_table, vocabulary)

    # weighted & embedding_column
    _add_weighted_embedding_columns(columns, weighted_features, feature_table, vocabulary)

    # 检查是否使用了feature_table中的所有特征
    encoded_features = cate_features + weighted_features
    unused_features = set(feature_table.keys()) - set(encoded_features)
    # if len(unused_features) != 0:
    #     raise ValueError("unused features: %s" % unused_features)

    return columns



if __name__ == "__main__":
    pass
