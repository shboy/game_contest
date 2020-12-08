import tensorflow as tf

"""
saved_model_cli show --dir /path/to/pb --all
"""


def infer_from_saved_model():
    pb_dir = r"D:\corpus\ai_push\guidance_push_samples_from_push_tf2\daily_model\dssm\a\20200226"
    pb_dir = r"D:\corpus\ai_push\guidance_push_samples_from_push_tf2\daily_model\general\x\1583055606"
    pb_dir = r"D:\corpus\ai_push_v2\daily_model\esmm-0728"
    imported = tf.saved_model.load(pb_dir)
    infer = imported.signatures["serving_default"]
    print(infer)
    print(infer.structured_outputs)

    test_path = "D:/corpus/ai_push/guidance_push_samples_from_push_tf2/date=20200218/dev/part-r-00000"
    data = tf.compat.v1.io.tf_record_iterator(test_path)
    for d in data:
        input = tf.convert_to_tensor([d])
        print("input: ", input)
        out = infer(input)
        print(out)


if __name__ == "__main__":
    infer_from_saved_model()
