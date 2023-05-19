import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
import tensorflow_datasets as tfds
import tensorflow_probability as tfp
import pandas as pd
tfk = tf.keras
tfd = tfp.distributions
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest

scaler = StandardScaler()
detector = IsolationForest(n_estimators=1000,contamination="auto", random_state=0)
#
# class MeanMetricWrapper(keras.metrics.Mean):
#   def __init__(self, fn, name=None, dtype=None, **kwargs):
#     super(MeanMetricWrapper, self).__init__(name=name, dtype=dtype)
#     self._fn = fn
#     self._fn_kwargs = kwargs
#   def update_state(self, y_true, y_pred, sample_weight=None):
#     matches = self._fn(y_true, y_pred, **self._fn_kwargs)
#     return super(MeanMetricWrapper, self).update_state(
#         matches, sample_weight=sample_weight)
#   def get_config(self):
#     config = {}
#     for k, v in six.iteritems(self._fn_kwargs):
#       config[k] = K.eval(v) if is_tensor_or_variable(v) else v
#     base_config = super(MeanMetricWrapper, self).get_config()
#     return dict(list(base_config.items()) + list(config.items()))

neg_log_likelihood =lambda x, rv_x: -rv_x.log_prob(x)

inputs = ["hr", "phr","slg","ba","pba","pslg"]
# inputs = ["park", "batter_hand", "pitcher_hand", "temp", "humidity", "wind", "ba", "slg", "hr", "pba", "pslg",
#             "phr"]
outputs = [0,1]

def test_shit(file):
    rank_1_tensor = tf.constant([2.0, 3.0, 4.0])
    print(rank_1_tensor)
    dataset = (
        tfds.load(name="wine_quality", as_supervised=True, split="train")
            .map(lambda x, y: (x, tf.cast(y, tf.float32)))
            .prefetch(buffer_size=dataset_size)
            .cache()
    )
    df = pd.read_csv(file)
    df_filtered = df[pd.isnull(df['ba']) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["pba"]) == False]
    features = ["park", "temp", "humidity", "wind", "hr", "phr"]
    target = df_filtered.pop('output')
    numeric_features = df_filtered[features]
    tf.convert_to_tensor(numeric_features)
    numeric_dataset = tf.data.Dataset.from_tensor_slices((numeric_features, target))

    print(type(numeric_dataset))
    print(type(dataset))
    for features, targets in dataset.take(1):
        print('Features: {}, Target: {}'.format(features, targets))

def get_dataset(file_path, train_size, batch_size=1, **kwargs, ):
    LABEL_COLUMN = 'output'
    dataset = tf.data.experimental.make_csv_dataset(
        file_path,
        batch_size=20,  # Artificially small to make examples easier to show.
        label_name=LABEL_COLUMN,
        na_value="?",
        num_epochs=1,
        ignore_errors=True,
        **kwargs)

    train_dataset = (
        dataset.take(train_size).shuffle(buffer_size=train_size).batch(batch_size)
    )
    test_dataset = dataset.skip(train_size).batch(batch_size)
    return train_dataset, test_dataset


def fix_csv(file):
    df = pd.read_csv(file)
    df_filtered = df[pd.isnull(df['ba']) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["pba"]) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["phr"]) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["hr"]) == False]
    for index, row in df_filtered.iterrows():
        df_filtered.at[index,"pba"] = pow(row["pba"],2)
        df_filtered.at[index,"pslg"] = pow(row["pba"],2)
        df_filtered.at[index,"phr"] = pow(row["pba"],2)
        df_filtered.at[index,"ba"] = pow(row["pba"],2)
        df_filtered.at[index,"hr"] = pow(row["pba"],2)
        df_filtered.at[index,"slg"] = pow(row["pba"],2)

    df_filtered.to_csv(file, index=False)


def load_from_csv(file, train_size, batch_size=1):
    df = pd.read_csv(file)
    df_filtered = df[pd.isnull(df['ba']) == False]
    df_filtered = df_filtered[pd.isnull(df_filtered["pba"]) == False]
    dataset = tf.data.Dataset.from_tensor_slices([[1, 2, 3],[3,4,5]])
    list(dataset.as_numpy_iterator())
    for features in dataset.take(5):
        print('Features: {}'.format(features))

    # features = ["park", "batter_hand", "pitcher_hand", "temp", "humidity", "wind", "ba", "slg", "hr", "pba", "pslg",
    #             "phr"]
    # features = ["park", "temp", "humidity", "wind", "hr", "phr"]
    features = ["hr", "phr","slg","ba","pba","pslg"]

    target = df_filtered.pop('output')
    numeric_features = df_filtered[features]
    tf.convert_to_tensor(numeric_features)
    numeric_dataset = tf.data.Dataset.from_tensor_slices((numeric_features, target))
    data_train = numeric_dataset.take(train_size).batch(batch_size).repeat(500)
    data_test = numeric_dataset.skip(train_size).batch(1)
    # print(numeric_features)
    # numeric_features = (
    #     tf.data.Dataset.from_tensor_slices(
    #         (
    #             df_filtered.values,
    #             target.values
    #         )
    #     )
    # )
    #
    # dataset = (
    #     tfds.load(name="wine_quality", as_supervised=True, split="train")
    #         .map(lambda x, y: (x, tf.cast(y, tf.float32)))
    #         .prefetch(buffer_size=dataset_size)
    #         .cache()
    # )
    return numeric_features, target, numeric_dataset, data_train, data_test
    # for features, targets in dataset.take(5):
    #     print('Features: {}, Target: {}'.format(features, targets))


# training_dataset = tf.data.Dataset.from_tensor_slices(dict(df_filtered), target.values)

    # train_dataset = (
    #     numeric_features.take(train_size).shuffle(buffer_size=train_size).batch(batch_size)
    # )
    # test_dataset = numeric_features.skip(train_size).batch(batch_size)

    return numeric_features, numeric_features,numeric_dataset


def get_train_and_test_splits(train_size, batch_size=1):
    # We prefetch with a buffer the same size as the dataset because th dataset
    # is very small and fits into memory.
    dataset = (
        tfds.load(name="wine_quality", as_supervised=True, split="train")
            .map(lambda x, y: (x, tf.cast(y, tf.float32)))
            .prefetch(buffer_size=dataset_size)
            .cache()
    )
    print(dataset)
    # We shuffle with a buffer the same size as the dataset.
    train_dataset = (
        dataset.take(train_size).shuffle(buffer_size=train_size).batch(batch_size)
    )
    test_dataset = dataset.skip(train_size).batch(batch_size)

    return train_dataset, test_dataset


hidden_units = [8, 8]
learning_rate = 0.001


def run_experiment(model, loss, train_dataset, test_dataset, num_epochs=50):
    model.compile(
        optimizer=keras.optimizers.RMSprop(learning_rate=learning_rate),
        loss=loss,
        metrics=[keras.metrics.RootMeanSquaredError()],
    )

    print("Start training the model...")
    model.fit(train_dataset, epochs=num_epochs, validation_data=test_dataset)
    print("Model training finished.")
    _, rmse = model.evaluate(train_dataset, verbose=0)
    print(f"Train RMSE: {round(rmse, 3)}")

    print("Evaluating model performance...")
    _, rmse = model.evaluate(test_dataset, verbose=0)
    print(f"Test RMSE: {round(rmse, 3)}")


FEATURE_NAMES = [
    "fixed acidity",
    "volatile acidity",
    "citric acid",
    "residual sugar",
    "chlorides",
    "free sulfur dioxide",
    "total sulfur dioxide",
    "density",
    "pH",
    "sulphates",
    "alcohol",
]
FEATURE_NAMES = ["park"]


def create_model_inputs():
    inputs = {}
    for feature_name in FEATURE_NAMES:
        inputs[feature_name] = layers.Input(
            name=feature_name, shape=(6902, 12), dtype=tf.float32
        )
    print(inputs)
    return inputs


def create_baseline_model(numeric_features):
    normalizer = tf.keras.layers.Normalization(axis=-1)
    print(numeric_features)
    normalizer.adapt(numeric_features)
    normalizer(numeric_features.iloc[:3])
    # features = keras.layers.concatenate(numeric_features)
    # features = layers.BatchNormalization()(features)

    model = tf.keras.Sequential([
        normalizer,
        tf.keras.layers.Dense(12, activation='sigmoid'),
        tf.keras.layers.Dense(12, activation="sigmoid"),
        tf.keras.layers.Dense(1)
    ])
    # for units in hidden_units:
    #     features = layers.Dense(units, activation="sigmoid")(features)
    #     # The output is deterministic: a single point estimate.
    # outputs = layers.Dense(units=1)(features)
    # model.outputs = outputs
    model.compile(optimizer=keras.optimizers.RMSprop(learning_rate=learning_rate),
                  loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
                  metrics=['accuracy'])
    return model

def make_model(data_train, n_batches):
    prior = tfd.Independent(tfd.Normal(loc=tf.zeros(len(outputs), dtype=tf.float32), scale=1.0),
                            reinterpreted_batch_ndims=1)
    model = tfk.Sequential([
        tfk.layers.InputLayer(input_shape=(len(inputs),), name="input"),
        tfk.layers.Dense(10, activation="relu", name="dense_1"),
        tfk.layers.Dense(tfp.layers.MultivariateNormalTriL.params_size(
            len(outputs)), activation=None, name="distribution_weights"),
        tfp.layers.MultivariateNormalTriL(len(outputs), activity_regularizer=tfp.layers.KLDivergenceRegularizer(prior,
                                                                                                                weight=1 / n_batches),
                                          name="output")
    ], name="model-2")
    model.compile(optimizer="adam")
    return model



dataset_size = 6000
batch_size = 256
n_epochs=500
train_size = int(dataset_size * 0.85)
file = "./testdata-cleaned_up_2022-03-29-2022-6-30-statcast.csv"
# test_shit(file)
fix_csv(file)
# train_dataset, test_dataset = get_dataset(file,train_size,  batch_size)
numeric_features, target, numeric_dataset, data_train, data_test = load_from_csv(file, train_size,
                                            batch_size)
#
# num_epochs = 100
# mse_loss = keras.losses.MeanSquaredError()
model = make_model(data_train, batch_size)
model.fit(data_train, epochs=n_epochs,verbose=True)
keras.models.save_model(model, "my_model-3.hdf5")
model.save_weights("ckpt-3")
# tf.keras.experimental.export_saved_model(model, './model.h5')

# reloaded_model = keras.models.load_model("./my_model.h5")
# reloaded_model = tf.keras.experimental.load_from_saved_model('./my_model.h5', custom_objects={'MultivariateNormalTriL':tfp.layers.MultivariateNormalTriL})
# print(reloaded_model.get_config())

# model = create_baseline_model(numeric_features)
# numeric_batches = numeric_dataset.shuffle(1000).batch(batch_size)
#
# model.fit(numeric_batches, epochs=200)
print("Evaluating model performance...")

# run_experiment(baseline_model, mse_loss, train_dataset, test_dataset)
#
sample = 100
examples, targets = list(data_test.shuffle(batch_size * 100).batch(sample))[
    0
]

predicted = model(examples)
for idx in range(sample):
    print(f"Predicted: {round(float(predicted[idx][0]), 1)} - Actual: {targets[idx]}")
