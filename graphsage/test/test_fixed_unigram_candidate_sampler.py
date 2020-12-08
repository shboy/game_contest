# -*- coding: utf-8 -*-
"""
@Time ： 2020/10/28 上午9:55
@Auth ： shenhao
@Email： shenhao@xiaomi.com
"""

import tensorflow as tf


def test1():
    vec = tf.constant([[1, 2, 3, 4, 6]], dtype=tf.int64)
    vec = tf.reshape(vec, [-1, 1])
    ids, _, _ = tf.nn.fixed_unigram_candidate_sampler(
        true_classes=vec,
        num_true=5,
        num_sampled=2,
        unique=False,
        # range_max=5,
        range_max=5,
        vocab_file='',
        distortion=1.0,
        num_reserved_ids=0,
        num_shards=1,
        shard=0,
        unigrams=(0.1, 0.2, 0.3, 0.1, 0.3),
        # unigrams=(1, 2, 3, 1, 3),
    )
    # vs = ids(vec)

    ids, _, _ = tf.nn.fixed_unigram_candidate_sampler(
        true_classes=vec,
        num_true=1,
        num_sampled=2,
        unique=False,
        range_max=5,
        vocab_file='',
        distortion=1.0,
        num_reserved_ids=0,
        num_shards=1,
        shard=0,
        unigrams=(0.1, 0.2, 0.3, 0.1, 0.3),
        # unigrams=(1, 2, 3, 1, 3),
    )
    with tf.Session() as sess:
        print(sess.run(ids))


if __name__ == '__main__':
    test1()