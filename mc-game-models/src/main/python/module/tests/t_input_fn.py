import time
import os, sys
import argparse
import tensorflow as tf
from absl import logging

logging.set_verbosity("info")

from rank.input_fn import train_input_fn


def t_norm():
    batch_size = 512
    feature_spec = {
        "QUERY_TOPIC": tf.io.VarLenFeature(tf.string),
        "DOMAIN_UV": tf.io.FixedLenFeature(shape=(1,), dtype=tf.int64),
        "LABEL": tf.io.FixedLenFeature(shape=(), dtype=tf.int64),
    }

    train_dirs = [r"D:\aiservice\recommend_model_data\quanzhi\training_examples\date=20201103\train"]
    ds_train = train_input_fn(train_dirs, feature_spec, batch_size=batch_size)
    for x, y in ds_train.take(10):
        print(x["QUERY_TOPIC"])
        print(y)

# def t_map_fn():
#     train_dirs = [r"D:\aiservice\recommend_model_data\quanzhi\training_examples\date=20201103\train"]
#     batch_size = 16
#     feature_spec = {
#         "QUERY_TOPIC": tf.io.VarLenFeature(tf.string),
#         "LABEL": tf.io.FixedLenFeature(shape=(), dtype=tf.int64),
#     }
#
#     def map_func(features, label):
#         eq = tf.squeeze(tf.equal(features['QUERY_TOPIC'], 'music_offline_recommend_new'))
#         wh = tf.where(eq)
#         idx = tf.squeeze(wh, axis=[1])  # idx must be 1-d
#         new_label = tf.gather(label, idx)
#         new_features = {}
#         for f, v in features.items():
#             assert not isinstance(v, tf.sparse.SparseTensor)
#             if not isinstance(v, tf.sparse.SparseTensor):
#                 new_features[f] = tf.gather(v, idx)
#         # print_op = tf.print("eq, wh, idx, new_label: ", eq, wh, idx, new_label)
#         # print_op = tf.print("eq ", eq, "wh ", wh, "idx ", idx, "new_label ", new_label)
#         print_op = tf.print()
#         with tf.control_dependencies([print_op]):
#             return new_features, new_label
#
#     ds_train = train_input_fn(train_dirs, feature_spec, batch_size=batch_size, repeat=False, batch_map_fn=map_func, use_dense_feature=True)
#     for i, (f, lb) in enumerate(ds_train):
#         print("batch ", i)
#         # print("QUERY_TOPIC: ", f['QUERY_TOPIC'].shape, end=" ")
#         # print("LABEL: ", lb.shape)
#         # assert f['QUERY_TOPIC'].shape[0] == batch_size
#         # assert lb.shape[0] == batch_size
#         if lb.shape[0] != batch_size:
#             print("error: ", lb.shape[0])


if __name__ == '__main__':
    t_norm()

