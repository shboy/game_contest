# -*- coding: utf-8 -*-
"""
@Time ： 2020/10/28 上午9:55
@Auth ： shenhao
@Email： shenhao@xiaomi.com
"""

import tensorflow as tf
import numpy as np


def test1():
    vec = tf.constant([[1, 2, 3, 5, 4, 6]], dtype=tf.int64)
    # ids = tf.reshape(vec, [2, ])
    # ids = tf.reshape(vec, [2, -1])
    ids = tf.reshape(vec, [6,])

    with tf.Session() as sess:
        print(sess.run(ids))



    c = np.random.random([5, 3])

    b = tf.nn.embedding_lookup(c, [1, 3])  ##查找数组中的序号为1和3的

    with tf.Session() as sess:

        sess.run(tf.initialize_all_variables())
        print(sess.run([b]))
        print(b.get_shape().as_list())
        print(c)


if __name__ == '__main__':
    test1()