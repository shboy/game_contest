import json
import tensorflow as tf
from absl import logging, flags

FLAGS = flags.FLAGS


def get_shard_info(tf_config):
    if tf_config is None:
        return 0, 1, "chief"
    else:
        config = json.loads(tf_config)
        num_workers = len(config['cluster']['worker'])
        num_chiefs = len(config['cluster']['chief'])
        shards = num_workers + num_chiefs
        task = config['task']
        if task['type'] == 'chief':
            shard_id = 0
        elif task['type'] == 'worker':
            shard_id = task['index'] + 1
        else:
            # evaluator & ps
            shard_id = 0
        return shard_id, shards, task['type']


def _parse_examples(example_protos, feature_spec, label_symbols=["LABEL"], use_dense_tensor=False):
    features = tf.io.parse_example(example_protos, feature_spec)

    if use_dense_tensor:
        dense_features = {}
        for name, raw_tensor in features.items():
            if isinstance(raw_tensor, tf.sparse.SparseTensor):
                dense_features[name] = tf.sparse.to_dense(raw_tensor)
            else:
                dense_features[name] = raw_tensor
        features = dense_features

    if len(label_symbols) == 1:
        label = features.pop(label_symbols[0])
        return features, label
    else:
        assert len(label_symbols) > 1
        label_dict = {}
        for lb in label_symbols:
            label_dict[lb] = features.pop(lb)
        return features, label_dict


def get_dataset(
        paths,
        feature_spec,
        batch_size,
        label_symbols=["LABEL"],
        use_dense_feature=False,
        batch_map_fn=None,
        shard_id=0,
        total_shards=1):
    """generate dataset
    Args:
        paths: list of file paths
        feature_spec: Feature spec for parsing example
        batch_size: Batch size
        use_dense_feature: A boolean identifying whether to convert sparse tensor to dense tensor
        batch_map_fn: A function mapping function

    Returns:
        Dataset
    """
    files = tf.data.Dataset.from_tensor_slices(paths)
    files = files.shard(total_shards, shard_id)

    def prepare_dataset(filename):
        print_name_op = tf.print("opening file:", filename)
        with tf.control_dependencies([print_name_op]):
            ds = tf.data.TFRecordDataset(filename, buffer_size=256 * 1024 * 1024, num_parallel_reads=8)
        ds = ds.batch(batch_size, drop_remainder=False)
        ds = ds.prefetch(1000)
        ds = ds.map(lambda lines: _parse_examples(lines, feature_spec, label_symbols, use_dense_feature),
                    num_parallel_calls=8)
        if batch_map_fn is not None:
            ds = ds.map(lambda x, y: batch_map_fn(x, y))
            ds = ds.unbatch().batch(batch_size)
        return ds

    dataset = files.flat_map(lambda x: prepare_dataset(x))
    return dataset


"""
flatten file dirs to a list of files
"""
def flat_files(file_dirs):
    files = []
    for file_dir in file_dirs:
        for file_name in tf.io.gfile.listdir(file_dir):
            if not file_name.startswith("part-"):
                continue
            files.append(file_dir + "/" + file_name)
    return files


def train_input_fn(train_dirs, feature_spec, batch_size, shard_id=0, total_shards=1,
                   label_symbols=["LABEL"], batch_map_fn=None, repeat=True, use_dense_feature=False):
    for lb in label_symbols:
        if lb not in feature_spec:
            feature_spec[lb] = tf.io.FixedLenFeature(shape=(1,), dtype=tf.int64, default_value=0)

    train_files = flat_files(train_dirs)
    dataset = get_dataset(train_files, feature_spec, batch_size, label_symbols, batch_map_fn=batch_map_fn,
                          use_dense_feature=use_dense_feature)
    if repeat:
        dataset = dataset.repeat()
    return dataset


def eval_input_fn(eval_dirs, feature_spec, batch_size, shard_id=0, total_shards=1,
                  label_symbols=["LABEL"], batch_map_fn=None, use_dense_feature=False):
    for lb in label_symbols:
        if lb not in feature_spec:
            feature_spec[lb] = tf.io.FixedLenFeature(shape=(1,), dtype=tf.int64, default_value=0)

    eval_files = flat_files(eval_dirs)
    dataset = get_dataset(eval_files, feature_spec, batch_size, label_symbols, batch_map_fn=batch_map_fn,
                          use_dense_feature=use_dense_feature)
    return dataset
