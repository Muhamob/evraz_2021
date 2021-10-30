from datetime import datetime

from sklearn.model_selection import train_test_split, KFold, cross_val_score
import pandas as pd
from evraz.features import DBFeatureExtractor
from evraz.metrics import sklearn_scorer, metric
from evraz.model import BaselineModel, LightAutoMLModel
from evraz.settings import Connection


def main():
    # Establish connection
    conn = Connection().open_conn().ping()

    # Extract features from db
    fe = DBFeatureExtractor(conn).fit()
    df = fe.transform(mode="train")
    print("NUmber of feature columns:", len(fe.feature_columns))

    cv = KFold(n_splits=5, random_state=42, shuffle=True)
    # train_df, val_df = train_test_split(df, shuffle=True, test_size=0.2, random_state=42)

    model = LightAutoMLModel()

    print(
        cross_val_score(
            estimator=model,
            X=df[fe.feature_columns],
            y=df[fe.target_columns],
            scoring=sklearn_scorer,
            cv=cv
        )
    )

    final_model = model.fit(
        X=df[fe.feature_columns],
        y=df[fe.target_columns],
    )

    submission_df = fe.transform(mode='test')
    predictions = final_model.predict(submission_df[fe.feature_columns])

    submission = (
        submission_df[['NPLV']]
        .assign(TST=predictions.TST)
        .assign(C=predictions.C)
        .astype({
            "NPLV": int
        })
        .sort_values("NPLV")
    )

    timestamp = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    submission.to_csv(f"../data/submissions/{timestamp}.csv", index=False)


if __name__ == "__main__":
    main()