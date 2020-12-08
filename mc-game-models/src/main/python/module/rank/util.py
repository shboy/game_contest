import numpy as np
from absl import logging
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers

def stat_model_parameters(trainable_variables):
    var_stat = {}
    for var in trainable_variables:
        var_num = np.prod(var.get_shape().as_list())
        var_stat[var.name] = var_num

    logging.info('The total number of parameters are: %d', sum(var_stat.values()))
    for var, var_num in sorted(var_stat.items(), key=lambda x: x[1], reverse=True):
        logging.info("%s: %d" % (var, var_num))


class W2vManager:
    def __init__(self, w2v_path):
        self.vocab = []
        self.embeddings = []
        w2v_file = tf.io.gfile.GFile(w2v_path)
        for i, line in enumerate(w2v_file.readlines()):
            if line.strip() == "":
                continue
            tokens = line.split()
            if len(tokens) != 201:
                continue
            self.vocab.append(tokens[0])
            self.embeddings.append([float(x) for x in tokens[1:]])
        self.embeddings = np.array(self.embeddings)
        logging.info("w2v vocabulary: %s", self.embeddings.shape)

        self.word2idx = dict((e, i) for i, e in enumerate(self.vocab))

    def sub_embeddings(self, word_list):
        unknown_words = []
        vocab = []
        indices = []
        for w in word_list:
            if w not in self.word2idx:
                unknown_words.append(w)
            else:
                vocab.append(w)
                indices.append(self.word2idx[w])
        return vocab, unknown_words, self.embeddings[indices]


class DNNModel(keras.Model):
    def __init__(self, units, use_bn=True, output_logits=False):
        super().__init__()
        self.model_layers = []
        for u in units:
            self.model_layers.append(layers.Dense(units=u))
            self.model_layers.append(layers.BatchNormalization())
            self.model_layers.append(layers.ReLU())

        if output_logits:
            self.model_layers.append(layers.Dense(units=1))
        else:
            self.model_layers.append(layers.Dense(units=1, activation=keras.activations.sigmoid))

    def call(self, inputs, training=False):
        o = inputs
        for _layer in self.model_layers:
            if isinstance(_layer, layers.BatchNormalization):
                o = _layer(o, training=False)
            else:
                o = _layer(o)
        return o


######################################################
# TEST
######################################################

def test_load_w2v():
    w2v_path = r""
    # w2v = W2vManager(w2v_path)
    print(w2v_path)


if __name__ == '__main__':
    test_load_w2v()
