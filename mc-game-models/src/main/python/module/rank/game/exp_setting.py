import os
from absl import flags, logging
from tensorflow import feature_column as fc

from rank.feature_table import get_feature_table
from rank.game.columns import create_columns
from .feature_config import FEATURE_CONFIGS

FLAGS = flags.FLAGS

def get_model_fn(model):
    model_scope = {}
    exec("from rank.%s import model_fn" % model, model_scope)
    return model_scope['model_fn']

def get_exp_params(model, exp_id, vocab):
    # features setting
    if exp_id == "-1":
        feature_table = get_feature_table(FEATURE_CONFIGS)
        column_fn = create_columns

    else:
        raise ValueError("not supported expID: %s" % exp_id)

    # 指定feature_names，用于特征选择
    if FLAGS.feature_names != "-1":
        feature_names = FLAGS.feature_names.split("#")
        feature_table = get_feature_table(FEATURE_CONFIGS, feature_names)

    # deepfm latent vector长度需要保证一致
    if model == "deepfm":
        for f in feature_table:
            feature_table[f].emb_width = FLAGS.latent_vec_dim

    feature_columns = column_fn(feature_table, vocab)
    exp_params = {
        "feature_table": feature_table,
        "feature_columns": feature_columns,
        "parse_spec": fc.make_parse_example_spec(feature_columns),
        "model_fn": get_model_fn(model)
    }

    return exp_params
