import tensorflow as tf
import json

from absl import flags, logging

FLAGS = flags.FLAGS


class Vocabulary:
    def __init__(self, vocab_path, is_hash=False):
        self.base_path = vocab_path
        self.stats = self.load_meta()
        self.vocab = self.load_vocab(is_hash)

    def load_meta(self):
        stats = {}
        stats_file_name = self.base_path + "/.meta-r-00000"
        feature_stats_file = tf.io.gfile.GFile(stats_file_name)
        for line in feature_stats_file.readlines():
            field_id, meta = line.strip().split("\t")
            stats[field_id] = json.loads(meta)
            logging.info("read meta for %s: %s" % (field_id, meta))
        feature_stats_file.close()
        return stats

    def get_vocabulary_path(self, feature_name):
        return self.base_path + "/vocabulary-" + feature_name + "-r-00000"

    def load_vocab(self, is_hash=False):
        vocab = {}
        for feature_name in self.stats.keys():
            if feature_name != "global":
                feature_vocab_file = tf.io.gfile.GFile(self.get_vocabulary_path(feature_name))
                if is_hash:
                    vocab[feature_name] = [int(line.strip()) for line in feature_vocab_file.readlines() if line.strip() != ""]
                else:
                    vocab[feature_name] = [line.strip() for line in feature_vocab_file.readlines() if line.strip() != ""]
                feature_vocab_file.close()
        return vocab


def test_vocab():
    logging.set_verbosity("info")
    # vocab_path = "D:/corpus/ai_push_v2/tf_record_data/quanzhi/date=20201013/vocabulary"
    vocab_path = "/home/mi/shenhao/work/xiaomi-dataming-b-2020/mc-game-models/python/data/training_examples/date=20200901/vocabulary"
    vocab = Vocabulary(vocab_path)
    print("finish")


if __name__ == "__main__":
    test_vocab()
