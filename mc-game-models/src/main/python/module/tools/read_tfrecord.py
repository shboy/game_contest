import os, sys
import tensorflow as tf
from collections import OrderedDict
import pickle


def read_tfrecord(file_dir_or_paths, max_samples=1e9):
    file_paths = ([file_dir_or_paths] if os.path.isfile(file_dir_or_paths)
                  else [file_dir_or_paths + "/" + name for name in os.listdir(file_dir_or_paths)])
    examples = []
    dataset = tf.data.TFRecordDataset(file_paths)
    for i, serialized_example in enumerate(dataset):
        if i > max_samples:
            break
        if i != 0 and i % 50000 == 0:
            print("read %d examples" % i)
        example = tf.train.Example()
        example.ParseFromString(serialized_example.numpy())
        features = example.features.feature

        sample_thumbnail_map = OrderedDict()

        for f, v in features.items():
            if len(v.float_list.value) != 0:
                val = list(v.float_list.value)
            elif len(v.bytes_list.value) != 0:
                val = [byte_str.decode("utf-8") for byte_str in v.bytes_list.value]
            else:
                val = list(v.int64_list.value)
            sample_thumbnail_map[f] = val
        examples.append(sample_thumbnail_map)
    return examples


def print_tfrecord_sample(file_dir_or_paths, max_samples=1e4, out_path=None):
    result = read_tfrecord(file_dir_or_paths, max_samples)
    feature_names = {f for sample in result for f in sample}

    need_close = False
    if out_path is None:
        out = sys.stdout
    else:
        out = open(out_path, "w", encoding='utf-8')
        need_close = True  # 怎么判断是文件流，不是标准输出流？ flush就可以了

    for i, sample in enumerate(result):
        print("\n###[%d]###\n" % i, end='', file=out)
        for k, v in sorted(sample.items()):
            print("%s: %s\n" % (k, v), end='', file=out)
    print("\n###END###\n")
    if need_close:
        out.close()
    print("features [%d]: \n%s" % (len(feature_names), "\n".join(sorted(feature_names))))


def filter_tfrecord_data(file_dir_or_paths, filter_fn, output_file, max_samples=1e9):
    """
    filter_fn: filter_fn(tf-example) -> boolean
    """
    file_paths = ([file_dir_or_paths] if os.path.isfile(file_dir_or_paths)
                  else [file_dir_or_paths + "/" + name for name in os.listdir(file_dir_or_paths)])
    examples = []
    dataset = tf.data.TFRecordDataset(file_paths)
    for i, serialized_example in enumerate(dataset):
        if i > max_samples:
            break
        if i != 0 and i % 50000 == 0:
            print("read %d examples" % i)
        example = tf.train.Example()
        example.ParseFromString(serialized_example.numpy())
        if filter_fn(example):
            examples.append(example)
    # Write the records to a file.
    with tf.io.TFRecordWriter(output_file) as file_writer:
        for example in examples:
            record_bytes = example.SerializeToString()
            file_writer.write(record_bytes)


def t_filter_tfrecord_data():
    sample_dir = r"D:/aiservice/recommend_model_data/quanzhi/training_examples/date=20201103"

    def filter_fn(example):
        features = example.features.feature
        if "QUERY_TOPIC" in features:
            v = features['QUERY_TOPIC']
            val = [byte_str.decode("utf-8") for byte_str in v.bytes_list.value]
            if val[0] == "season_recipe_reminder":
                return True
        return False

    output_path = sample_dir + "/dev.tfrecord"
    filter_tfrecord_data(sample_dir + "/dev", filter_fn, output_path, max_samples=1e9)
    print_tfrecord_sample(output_path, 100)


def t_read_tfrecord_pkl():
    import time
    sample_dir = r"D:/aiservice/recommend_model_data/quanzhi/training_examples/date=20201103"
    pickle_path = sample_dir + "/all_train_examples.pkl"
    t0 = time.time()
    if os.path.exists(pickle_path):
        with open(pickle_path, "rb") as fin:
            examples = pickle.load(fin)
    else:
        with open(pickle_path, "wb") as fout:
            examples = read_tfrecord(sample_dir + "/train")
            pickle.dump(examples, fout)
    print("cost: %d s" % (time.time() - t0))


def t_print_tfrecord_sample():
    # sample_dir = r"D:/aiservice/recommend_model_data/quanzhi/training_examples/date=20201103/train"
    sample_dir = "/home/mi/shenhao/work/xiaomi-dataming-b-2020/mc-game-models/src/main/python/data/game/training_examples/date=20200901/train/part-r-00499"
    print_tfrecord_sample(sample_dir, 100)


if __name__ == "__main__":
    t_print_tfrecord_sample()
