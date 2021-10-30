import os

from sklearn.model_selection import KFold, cross_val_score

from evraz.features import DBFeatureExtractor
from evraz.metrics import sklearn_scorer
from evraz.model import LightAutoMLModel
from evraz.settings import Connection


def main():
    # Establish connection
    conn = Connection().open_conn().ping()

    # Extract features from db
    fe = DBFeatureExtractor(conn).fit()
    df = fe.transform(mode="train")
    print("NUmber of feature columns:", len(fe.feature_columns))

    cv = KFold(n_splits=5, random_state=42, shuffle=True)

    model = LightAutoMLModel(automl_params={
        'timeout': 60,
        'general_params': {
            'use_algos': [['cb', 'cb_tuned']]
        }
    })

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

    filename = os.environ.get("SUB_NAME")
    submission.to_csv(f"../data/submissions/{filename}.csv", index=False)


if __name__ == "__main__":
    main()
