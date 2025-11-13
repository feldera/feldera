from feldera import PipelineBuilder, FelderaClient
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
import argparse
from argparse import RawTextHelpFormatter
import time
import json

DEFAULT_API_URL = "http://localhost:8080"

# How long to run the inference pipeline for.
INFERENCE_TIME_SECONDS = 60


def main():
    parser = argparse.ArgumentParser(
        formatter_class=RawTextHelpFormatter,
        description="""Real-time fraud detection demo.

This script demonstrates the use of Feldera for real-time feature engineering on top of Delta Lake.
It is based on the following blog post: https://www.feldera.com/blog/feature-engineering-part2/.

The script first creates a Feldera pipeline that reads a snapshot of labeled historical data from a
Delta Lake and derives feature vectors from it. It retrieves the computed feature vectors into
a Pandas dataframe and uses them to train an ML model to detect fraudulent credit card transactions.

Next, the script creates the second pipeline that simulates real-time feature computation by scanning
the transaction log of another Delta table. It reads computed features via HTTP and feeds them to the
trained ML model for inference.

By default, this script connects to a Feldera instance at http://localhost:8080.  See
https://github.com/feldera/feldera#quick-start-with-docker for instructions to run Feldera in a
Docker container.  Alternatively, use --api-url to specify a different Feldera instance, e.g.,
--api-url=https://try.feldera.com.

The script reads input tables from public S3 buckets and does not require S3 credentials by default.
Additionally, it can be configured to write outputs to Delta tables stored in S3, which requires
specifying an AWS access key and region.
""",
        epilog="""Examples:

# Run the script using default configuration settings. Feldera must be running on localhost:8080.
> python run.py

# Run against the Feldera online sandbox.
> python run.py --api-url=https://try.feldera.com --api-key=apikey:ABCD123....

# Write outputs to s3://feldera-fraud-detection-demo/feature_train and s3://feldera-fraud-detection-demo/feature_infer.
> python run.py --deltalake-uri="s3://feldera-fraud-detection-demo" --aws-access-key-id=$AWS_ACCESS_KEY_ID --aws-secret-access-key=$AWS_SECRET_ACCESS_KEY --aws-region='us-east-1'
""",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Feldera API URL (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--api-key",
        help="Feldera API key. When running against the Feldera public sandbox ('--api-url=https://try.feldera.com'),\n"
        "use the 'Settings' menu at try.feldera.com to generate an API key.\n"
        "Example: --api-key=apikey:ABCD....",
    )
    parser.add_argument(
        "--deltalake-uri",
        help="When specified, the script configures the Feldera pipeline to write feature vectors computed during model training and\n"
        "inference to Delta tables under '<delta-lake-uri>/feature_train/' and '<delta-lake-uri>/feature_infer/' respectively.\n"
        "Setting this option requires configuring --aws-access-key-id, --aws-secret-access-key, and --aws-region.\n"
        "Example: --delta-lake-uri=s3://feldera-fraud-detection-demo",
    )
    parser.add_argument(
        "--aws-access-key-id",
        help="AWS access key id used to access the S3 bucket at the location specified by 'deltalake-uri'.\n"
        "The AIM user that this key belongs must have write access to the S3 bucket.",
    )
    parser.add_argument(
        "--aws-secret-access-key",
        help="AWS secret access key used to access the S3 bucket at the location specified by 'delta-lake-uri'",
    )
    parser.add_argument(
        "--aws-region",
        help="AWS region associated with 'deltalake-uri', e.g., 'us-east-1'.",
    )

    args = parser.parse_args()

    print(
        f"\nRunning the training pipeline. Point your browser to {
            args.api_url
        }/pipelines/fraud_detection_training/ to monitor the status of the pipeline."
    )

    client = FelderaClient(args.api_url, api_key=args.api_key)

    # S3 access credentials for output delta tables.
    s3_credentials = {}
    if args.aws_access_key_id is not None:
        s3_credentials = s3_credentials | {"aws_access_key_id": args.aws_access_key_id}

    if args.aws_secret_access_key is not None:
        s3_credentials = s3_credentials | {
            "aws_secret_access_key": args.aws_secret_access_key
        }

    if args.aws_region is not None:
        s3_credentials = s3_credentials | {"aws_region": args.aws_region}

    # Load DEMOGRAPHICS data from a Delta table stored in an S3 bucket.
    demographics_connectors = [
        {
            "name": "demographics",
            "transport": {
                "name": "delta_table_input",
                "config": {
                    "uri": "s3://feldera-fraud-detection-data/demographics_train",
                    "mode": "snapshot",
                    "aws_skip_signature": "true",
                },
            },
        }
    ]

    # Load credit card TRANSACTION data.
    transactions_connectors = [
        {
            "name": "transactions",
            "transport": {
                "name": "delta_table_input",
                "config": {
                    "uri": "s3://feldera-fraud-detection-data/transaction_train",
                    "mode": "snapshot",
                    "aws_skip_signature": "true",
                    "timestamp_column": "unix_time",
                },
            },
        }
    ]

    # Write computed feature vectors to another delta table.
    if args.deltalake_uri is not None:
        features_connectors = [
            {
                "name": "delta",
                "transport": {
                    "name": "delta_table_output",
                    "config": {
                        "uri": f"{args.deltalake_uri}/feature_train",
                        "mode": "truncate",
                    }
                    | s3_credentials,
                },
            }
        ]
    else:
        features_connectors = []

    sql = build_program(
        json.dumps(transactions_connectors),
        json.dumps(demographics_connectors),
        json.dumps(features_connectors),
    )

    pipeline = PipelineBuilder(
        client, name="fraud_detection_training", sql=sql
    ).create_or_replace()

    hfeature = pipeline.listen("feature")

    # Process full snapshot of the input tables and compute a dataset
    # with feature vectors for use in model training and testing.
    pipeline.start()
    pipeline.wait_for_completion(force_stop=True)

    features_pd = hfeature.to_pandas()
    print(f"Computed {len(features_pd)} feature vectors")

    print("Training the model")

    feature_cols = list(features_pd.columns.drop("is_fraud"))

    config = {
        "feature_cols": feature_cols,
        "target_col": ["is_fraud"],
        "random_seed": 45,
        "train_test_split_ratio": 0.8,
    }

    trained_model, X_test, y_test = train_model(features_pd, config)

    print("Testing the trained model")

    y_pred = trained_model.predict(X_test)
    eval_metrics(y_test, y_pred)

    print(f"\nRunning the inference pipeline for {INFERENCE_TIME_SECONDS} seconds")

    # Load DEMOGRAPHICS data from a Delta table.
    demographics_connectors = [
        {
            "name": "demographics_delta",
            "transport": {
                "name": "delta_table_input",
                "config": {
                    "uri": "s3://feldera-fraud-detection-data/demographics_infer",
                    "mode": "snapshot",
                    "aws_skip_signature": "true",
                },
            },
        }
    ]

    # Read TRANSACTION data from a Delta table.
    # Configure the Delta Lake connector to read the initial snapshot of
    # the table before following the stream of changes in its transaction log.
    transactions_connectors = [
        {
            "name": "transactions_delta",
            "transport": {
                "name": "delta_table_input",
                "config": {
                    "uri": "s3://feldera-fraud-detection-data/transaction_infer",
                    "mode": "snapshot_and_follow",
                    "version": 10,
                    "timestamp_column": "unix_time",
                    "aws_skip_signature": "true",
                },
            },
        }
    ]

    # Store computed feature vectors in another delta table.
    if args.deltalake_uri is not None:
        features_connectors = [
            {
                "name": "deltalake_uri",
                "transport": {
                    "name": "delta_table_output",
                    "config": {
                        "uri": f"{args.deltalake_uri}/feature_infer",
                        "mode": "truncate",
                    }
                    | s3_credentials,
                },
            }
        ]
    else:
        features_connectors = []

    sql = build_program(
        json.dumps(transactions_connectors),
        json.dumps(demographics_connectors),
        json.dumps(features_connectors),
    )
    pipeline = PipelineBuilder(
        client, name="fraud_detection_inference", sql=sql
    ).create_or_replace()

    pipeline.foreach_chunk("feature", lambda df, chunk: inference(trained_model, df))

    # Start the pipeline to continuously process the input stream of credit card
    # transactions and output newly computed feature vectors to a Delta table.

    pipeline.start()

    time.sleep(INFERENCE_TIME_SECONDS)

    print(
        f"Shutting down the inference pipeline after {INFERENCE_TIME_SECONDS} seconds"
    )
    pipeline.stop(force=True)


def build_program(
    transactions_connectors: str, demographics_connectors: str, features_connectors: str
) -> str:
    return f"""-- Credit card transactions
    CREATE TABLE TRANSACTION(
        trans_date_trans_time TIMESTAMP,
        cc_num BIGINT,
        merchant STRING,
        category STRING,
        amt DECIMAL(38, 2),
        trans_num STRING,
        unix_time BIGINT LATENESS 3600 * 24 * 30,
        merch_lat DOUBLE,
        merch_long DOUBLE,
        is_fraud BIGINT
    ) WITH ('connectors' = '{transactions_connectors}');

    -- Demographics data.
    CREATE TABLE DEMOGRAPHICS(
        cc_num BIGINT,
        first STRING,
        last STRING,
        gender STRING,
        street STRING,
        city STRING,
        state STRING,
        zip BIGINT,
        lat DOUBLE,
        long DOUBLE,
        city_pop BIGINT,
        job STRING,
        dob DATE
    ) WITH ('connectors' = '{demographics_connectors}');

    -- Feature query written in the Feldera SQL dialect.
    CREATE VIEW FEATURE
    WITH ('connectors' = '{features_connectors}')
    AS
        SELECT
           t.cc_num,
           dayofweek(trans_date_trans_time) as d,
           CASE
             WHEN dayofweek(trans_date_trans_time) IN(6, 7) THEN true
             ELSE false
           END AS is_weekend,
           hour(trans_date_trans_time) as hour_of_day,
           CASE
             WHEN hour(trans_date_trans_time) <= 6 THEN true
             ELSE false
           END AS is_night,
           -- Average spending per day, per week, and per month.
           AVG(amt) OVER window_1_day AS avg_spend_pd,
           AVG(amt) OVER window_7_day AS avg_spend_pw,
           AVG(amt) OVER window_30_day AS avg_spend_pm,
           -- Average spending over the last three months for the same day of the week.
           COALESCE(
            AVG(amt) OVER (
              PARTITION BY t.cc_num, EXTRACT(DAY FROM trans_date_trans_time)
              ORDER BY unix_time
              RANGE BETWEEN 7776000 PRECEDING and CURRENT ROW
            ), 0) AS avg_spend_p3m_over_d,
           -- Number of transactions in the last 24 hours.
           COUNT(*) OVER window_1_day AS trans_freq_24,
           amt, unix_time, zip, city_pop, is_fraud
        FROM transaction as t
        JOIN demographics as d
        ON t.cc_num = d.cc_num
        WINDOW
          window_1_day AS (PARTITION BY t.cc_num ORDER BY unix_time RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW),
          window_7_day AS (PARTITION BY t.cc_num ORDER BY unix_time RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW),
          window_30_day AS (PARTITION BY t.cc_num ORDER BY unix_time RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW);
      """


# Split input dataframe into train and test sets
def get_train_test_data(
    dataframe, feature_cols, target_col, train_test_split_ratio, random_seed
):
    X = dataframe[feature_cols]
    y = dataframe[target_col]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, train_size=train_test_split_ratio, random_state=random_seed
    )

    return X_train, X_test, y_train, y_test


# Train a decision tree classifier using xgboost.
# Other ML frameworks and types of ML models can be readily used with Feldera.
def train_model(dataframe, config):
    max_depth = 12
    n_estimators = 100

    X_train, X_test, y_train, y_test = get_train_test_data(
        dataframe,
        config["feature_cols"],
        config["target_col"],
        config["train_test_split_ratio"],
        config["random_seed"],
    )

    model = XGBClassifier(
        max_depth=max_depth, n_estimators=n_estimators, objective="binary:logistic"
    )

    model.fit(X_train, y_train.values.ravel())
    return model, X_test, y_test


# Evaluate prediction accuracy against ground truth.
def eval_metrics(y, predictions):
    cm = confusion_matrix(y, predictions)
    print("Confusion matrix:")
    print(cm)

    if len(cm) < 2 or cm[1][1] == 0:  # checking if there are no true positives
        print("No fraudulent transaction to evaluate")
        return
    else:
        precision = cm[1][1] / (cm[1][1] + cm[0][1])
        recall = cm[1][1] / (cm[1][1] + cm[1][0])
        f1 = 2 * (precision * recall) / (precision + recall)

    print(f"Precision: {precision * 100:.2f}%")
    print(f"Recall: {recall * 100:.2f}%")
    print(f"F1 Score: {f1 * 100:.2f}%")


def inference(trained_model, df):
    print(f"\nReceived {len(df)} feature vectors.")
    if len(df) == 0:
        return

    feature_cols_inf = list(df.columns.drop("is_fraud"))
    X_inf = df[feature_cols_inf].values  # convert to numpy array
    y_inf = df["is_fraud"].values
    predictions_inf = trained_model.predict(X_inf)

    eval_metrics(y_inf, predictions_inf)


if __name__ == "__main__":
    main()
