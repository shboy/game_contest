import os
import sys
import tensorflow as tf
from absl import app, flags, logging
logging.set_verbosity('info')
logging.info("tf version: %s", tf.version.VERSION)

os.environ["CUDA_VISIBLE_DEVICES"]="-1"

if os.path.dirname(os.path.dirname(os.path.abspath(__file__))) not in sys.path:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rank import input_fn
from rank.vocab import Vocabulary

flags.DEFINE_string("model", "deep", "including deep and dssm")
flags.DEFINE_string("app_name", "game", "application name")
# flags.DEFINE_string("data_path", "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/recommend_model_data", "The base hdfs path of training data")
flags.DEFINE_string("data_path", "/home/mi/shenhao/work/xiaomi-dataming-b-2020/mc-game-models/src/main/python/data", "The base hdfs path of training data")
flags.DEFINE_string("base_date", "20200901", "date", short_name="d")
flags.DEFINE_string("model_dir", "-1", "model dir")
flags.DEFINE_integer("batch_size", 512, "batch size")
flags.DEFINE_float("learning_rate", 2e-4, "learning rate")
flags.DEFINE_integer("train_max_steps", 300000, "max traning steps", short_name="ms")
flags.DEFINE_integer("checkpoint_steps", 50000, "steps until next checkpoint", short_name="cs")
flags.DEFINE_integer("eval_steps", -1, "evaluation steps", short_name="es")
flags.DEFINE_integer("throttle_secs", 6, "batch size")
flags.DEFINE_integer("decay_steps", 300000, "decay steps")
flags.DEFINE_float("decay_coef", 0.95, "decay coeffeciency")
flags.DEFINE_float("dropout_rate", 0.3, "dropout rate")
flags.DEFINE_integer("shuffle_size", 50000, "shuffle among n samples during input")
flags.DEFINE_string("eval_paths", "-1", "eval paths")  # eg.,path1,path2,path3
flags.DEFINE_string("eval_dates", "-1", "eval dates")  # eg.,20191212_20191213_20191214
flags.DEFINE_string("experiment_id", "-1", "experiment id", short_name="i")
flags.DEFINE_bool("evaluation", True, "is this training a evaluation task(True) or production task(False)")
flags.DEFINE_bool("only_eval", False, "no need to train")  # just for evaluation using existed model, not save pb
flags.DEFINE_string("feature_names", "-1", "feature names joined by #")  # eg.,USER_MI_PUSH_CLICK_DAYS#QUERY_FUNC
flags.DEFINE_bool("use_bn", True, "use BatchNormalization")
flags.DEFINE_float("alpha_weight", -1, "alpha_weight")
flags.DEFINE_bool("del_existed_model", True, "del_existed_model")
flags.DEFINE_bool("is_hash_feature", False, "hash or raw feature")
flags.DEFINE_bool("eval_all_user_types", False, "whether to eval all user types")
flags.DEFINE_string("train_user_type", "ALL", "training set using which user type")
flags.DEFINE_string("dnn_units", "256_128_128", "dnn units")
flags.DEFINE_string("w2v_path", "hdfs://zjyprc-hadoop/user/h_data_platform/platform/aiservice/guidance_push_base_example_v2_experiment/word_embedding_train", "w2v hdfs address")

# deepfm
flags.DEFINE_integer("latent_vec_dim", 16, "the dimension of latent vector")

FLAGS = flags.FLAGS


def _parse_paths(suffix=''):
    data_path = "%s/%s/training_examples/date=%s" % (FLAGS.data_path, FLAGS.app_name, FLAGS.base_date)
    if FLAGS.model_dir == "-1":
        model_path = r"%s/model_%s_%s" % (data_path, FLAGS.model, FLAGS.experiment_id)
    elif FLAGS.model_dir.find("/") == -1:  # model文件夹名
        model_path = data_path + "/" + FLAGS.model_dir
    else:
        model_path = FLAGS.model_dir

    # tf.io.gfile.makedirs may fail on windows if the path contains both '/' and '\'
    if sys.platform == "win32":
        model_path = model_path.replace('/', '\\')
    vocab_path = data_path + "/vocabulary" + suffix
    train_dirs = [data_path + "/train" + suffix]
    if FLAGS.eval_paths == "-1" and FLAGS.eval_dates == "-1":
        eval_dirs = [data_path + "/dev"]
    elif FLAGS.eval_dates != "-1":  # 20191212_20191213_20191214
        eval_dirs = [FLAGS.data_path + "/date=%s/dev" % date for date in FLAGS.eval_dates.split("_")]
    else:  # "path1,path2"
        eval_dirs = [FLAGS.eval_paths.split(",")]
    return train_dirs, vocab_path, eval_dirs, model_path


def build_serving_fn(feature_spec):
    if "LABEL" in feature_spec:
        feature_spec.pop("LABEL")
    if "CONVERSION_LABEL" in feature_spec:
        feature_spec.pop("CONVERSION_LABEL")
    return tf.estimator.export.build_parsing_serving_input_receiver_fn(feature_spec)

def auc_larger(best_eval_result, current_eval_result):
    if not best_eval_result or "auc" not in best_eval_result:
        raise ValueError('best_eval_result cannot be empty or no auc is found in it.')
    if not current_eval_result or "auc" not in current_eval_result:
        raise ValueError('current_eval_result cannot be empty or no auc is found in it.')
    return current_eval_result['auc'] > best_eval_result['auc']

def get_app_exp_params_fn(app_name):
    app_scope = {}
    exec("from rank.%s.exp_setting import get_exp_params" % app_name, app_scope)
    return app_scope['get_exp_params']


def main(unused_argv):
    logging.info("app_name: %s; using model: %s; expID=%s", FLAGS.app_name, FLAGS.model, FLAGS.experiment_id)

    train_dirs, vocab_path, eval_dirs, model_path = _parse_paths()

    logging.info("resolved paths:")
    logging.info("train dirs: %s", train_dirs)
    logging.info("vocab path: %s", vocab_path)
    logging.info("eval dirs: %s", eval_dirs)
    logging.info("model path: %s", model_path)

    # exp_setting
    get_exp_params = get_app_exp_params_fn(FLAGS.app_name)
    # feature_config
    exp_params = get_exp_params(FLAGS.model, FLAGS.experiment_id, Vocabulary(vocab_path, FLAGS.is_hash_feature))

    feature_table = exp_params.pop("feature_table")
    parse_spec = exp_params.pop("parse_spec")
    model_fn = exp_params.pop("model_fn")

    logging.info("============feature table:============")
    item_features = sorted([f.name for f in feature_table.values() if f.group == "ITEM"])
    user_context_features = sorted([f.name for f in feature_table.values() if f.group == "USER_CONTEXT"])
    logging.info("USER_CONTEXT[%d]: %s", len(user_context_features), ", ".join(user_context_features))
    logging.info("ITEMS[%d]: %s", len(item_features), ", ".join(item_features))
    logging.info("======================================")

    logging.info("============feature spec:============")
    for f in sorted(parse_spec):
        logging.info("[%s] %s" % (f, parse_spec[f]))
    logging.info("======================================")

    if 'TF_CONFIG' in os.environ:
        shard_id, shards, shard_type = input_fn.get_shard_info(os.environ['TF_CONFIG'])
        logging.info("resolved %d shards, and shard id for this %s node is %d", shards, shard_type, shard_id)
    else:
        shard_id, shards = 0, 1
        logging.info("resolved standalone or local training, no sharding")

    if FLAGS.del_existed_model and tf.io.gfile.exists(model_path):
        logging.info("delete the existed model %s", model_path)
        tf.io.gfile.rmtree(model_path)

    if FLAGS.model == "esmm":
        label_symbols = ['LABEL', 'CONVERSION_LABEL']
    else:
        label_symbols = ['LABEL']

    train_spec = tf.estimator.TrainSpec(lambda: input_fn.train_input_fn(train_dirs, parse_spec, FLAGS.batch_size, shard_id, shards, label_symbols=label_symbols, use_dense_feature=True),
                                        max_steps=FLAGS.train_max_steps
                                        )

    best_exporter = tf.estimator.BestExporter(serving_input_receiver_fn=build_serving_fn(parse_spec),
                                              compare_fn=auc_larger,
                                              exports_to_keep=1)

    eval_spec = tf.estimator.EvalSpec(lambda: input_fn.eval_input_fn(eval_dirs, parse_spec, FLAGS.batch_size, shard_id, shards, label_symbols=label_symbols, use_dense_feature=True),
                                      steps=None if FLAGS.eval_steps < 0 else FLAGS.eval_steps,
                                      # exporters=best_exporter,
                                      throttle_secs=FLAGS.throttle_secs
                                      )

    run_config = tf.estimator.RunConfig(model_dir=model_path, save_checkpoints_steps=FLAGS.checkpoint_steps)

    classifier = tf.estimator.Estimator(
        model_fn=model_fn,
        config=run_config,
        params=exp_params
    )

    tf.estimator.train_and_evaluate(classifier, train_spec, eval_spec)

    if 'TF_CONFIG' not in os.environ or (shard_type == "chief" and shard_id == 0):
        if FLAGS.evaluation:
            logging.info("begin the final evaluation ...")
            metrics = classifier.evaluate(lambda: input_fn.eval_input_fn(eval_dirs, parse_spec, FLAGS.batch_size, label_symbols=label_symbols))
            logging.info("final evaluation: %s", metrics)
        saved_model_path = model_path if sys.platform == "win32" else model_path + "/final_model/default"
        classifier.export_saved_model(saved_model_path, build_serving_fn(parse_spec))


if __name__ == '__main__':
    os.environ['TZ'] = 'Asia/Shanghai'
    logging.info("arguments = %s" % str(sys.argv))
    app.run(main)
