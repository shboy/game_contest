import tensorflow as tf
from tensorflow import feature_column as fc
from rank.feature_table import *

from absl import flags, logging
FLAGS = flags.FLAGS

_WEIGHTED_SUFFIX = "_VALUE"
MIN_RANGE = 0.000001 # 为了将0单独分桶，因为0表示未知。


BUCKET_BOUNDARIES = {
    # 'USER_DOMAIN_PV': [MIN_RANGE, 1.01, 11.01, 21.01, 33.01, 47.01, 65.01, 91.01, 140.01, 15327.0],
    # 'USER_TOPIC_PV': [MIN_RANGE, 1.01, 3.01, 4.01, 5.01, 7.01, 44.0],
    # 'USER_TOPIC_ACCEPT': [MIN_RANGE, 2.01, 3.01, 4.01, 5.01, 6.01, 7.01, 8.01, 9.01, 10.01, 12.01, 15.01, 18.01, 20.01, 25.01],
    # 'USER_TOPIC_ACCEPT_RATE': [MIN_RANGE, 0.0217, 0.167, 0.2, 0.25, 0.333, 0.4, 0.5, 0.667, 1.0],
    # 'DOMAIN_UV': [MIN_RANGE, 15503.999, 164348.0, 293176.0, 633404.0, 914848.0, 1453470.0, 3110576.0, 3140199.0, 3141358.0],
    # 'TOPIC_PV': [MIN_RANGE, 2.01, 102086.0, 288734.0, 526570.0, 1211900.0, 1282462.0, 3459278.0, 3652605.0, 3675494.0],
    # 'TOPIC_ACCEPT': [MIN_RANGE, 10, 20, 50, 100, 200, 500, 1000, 5000, 10000, 50000, 100000, 200000, 500000],
    # 'TOPIC_ACCEPT_RATE': [MIN_RANGE, 0.001, 0.139, 0.161, 0.163, 0.164, 0.165, 0.178, 0.191, 0.216],
}


def _add_bucketed_columns(columns, features, feature_table, vocabulary):
    for f in features:
        assert f in feature_table
        # 如果是fixed_len的list特征
        if feature_table[f].feature_spec.is_list and feature_table[f].feature_spec.fixed:
            size = feature_table[f].feature_spec.size
            if feature_table[f].feature_spec.dtype == "int":
                numeric_col = fc.numeric_column(f, shape=(size, ), dtype=tf.int64, default_value=0)
            else:
                numeric_col = fc.numeric_column(f, shape=(size, ), default_value=0)
        # 如果不是list特征
        else:
            if feature_table[f].feature_spec.dtype == "int":
                numeric_col = fc.numeric_column(f, dtype=tf.int64, default_value=0)
            else:
                numeric_col = fc.numeric_column(f, default_value=0)
        bucketed_col = fc.bucketized_column(numeric_col, boundaries=BUCKET_BOUNDARIES[f])
        embedding_col = fc.embedding_column(bucketed_col, feature_table[f].emb_width, combiner='sqrtn')
        columns.append(embedding_col)

def _add_numeric_columns(columns, features, feature_table, vocabulary):
    for f in features:
        assert f in feature_table
        if feature_table[f].feature_spec.is_list and feature_table[f].feature_spec.fixed:
            size = feature_table[f].feature_spec.size
            if feature_table[f].feature_spec.dtype == "int":
                numeric_col = fc.numeric_column(f, shape=(size, ), dtype=tf.int64, default_value=0)
            else:
                numeric_col = fc.numeric_column(f, shape=(size, ), default_value=0)
        else:
            if feature_table[f].feature_spec.dtype == "int":
                numeric_col = fc.numeric_column(f, dtype=tf.int64, default_value=0)
            else:
                numeric_col = fc.numeric_column(f, default_value=0)
        # bucketed_col = fc.bucketized_column(numeric_col, boundaries=BUCKET_BOUNDARIES[f])
        # embedding_col = fc.embedding_column(bucketed_col, feature_table[f].emb_width, combiner='sqrtn')
        # columns.append(embedding_col)
        columns.append(numeric_col)


def _add_embedding_columns(columns, features, feature_table, vocabulary):
    for f in features:
        assert f in feature_table
        cate_col = fc.categorical_column_with_vocabulary_list(f, vocabulary.vocab[f])
        column = fc.embedding_column(cate_col, feature_table[f].emb_width, combiner='sqrtn')
        columns.append(column)


def _add_weighted_embedding_columns(columns, features, feature_table, vocabulary):
    for f in features:
        assert f in feature_table
        weighted_column = fc.weighted_categorical_column(
            fc.categorical_column_with_vocabulary_list(f, vocabulary.vocab[f]),
            f + _WEIGHTED_SUFFIX
        )
        emb_weighted_column = fc.embedding_column(weighted_column, feature_table[f].emb_width, combiner='sqrtn')
        columns.append(emb_weighted_column)


def create_columns(feature_table, vocabulary, group=None):
    columns = []

    cate_features = [f for f in feature_table if feature_table[f].feature_spec.dtype == "string"]
    numeric_features = [f for f in feature_table if feature_table[f].feature_spec.dtype in {"int", "float"}]

    # cate_features = ['MASTER_DOMAIN', 'EXPOSE_TIME', 'QUERY_DOMAIN', 'QUERY_TOPIC']

    # embedding_column
    _add_embedding_columns(columns, cate_features, feature_table, vocabulary)

    # bucketed & embedding_column
    # _add_bucketed_columns(columns, numeric_features, feature_table, vocabulary)

    _add_numeric_columns(columns, numeric_features, feature_table, vocabulary)

    return columns



if __name__ == "__main__":
    pass
