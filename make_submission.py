from datetime import datetime

from sklearn.model_selection import train_test_split, KFold, cross_val_score

from evraz.features import DBFeatureExtractor
from evraz.metrics import sklearn_scorer
from evraz.model import BaselineModel
from evraz.settings import Connection

if __name__ == "__main__":
    # Establish connection
    conn = Connection().open_conn().ping()

    # Extract features from db
    fe = DBFeatureExtractor(conn).fit()
    df = fe.transform(mode="train")

    cv = KFold(n_splits=5, random_state=42, shuffle=True)
    train_df, val_df = train_test_split(df, shuffle=True, test_size=0.2, random_state=42)

    model = BaselineModel({
        "depth": 5,
        'cat_features': fe.cat_columns,
    })

    print(cross_val_score(estimator=model,
                    X=train_df[fe.feature_columns],
                    y=train_df[fe.target_columns],
                    scoring=sklearn_scorer,
                    cv=cv,
                    fit_params={
                        'eval_set': [(val_df[fe.feature_columns], val_df[fe.target_columns]), ],
                        'silent': True
                    }))

    final_model = model.fit(
        X=train_df[fe.feature_columns],
        y=train_df[fe.target_columns],
        eval_set=[(val_df[fe.feature_columns], val_df[fe.target_columns]), ],
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
    )

    timestamp = datetime.today().strftime('%Y-%m-%d-%H:%M:%S')
    submission.to_csv(f"../data/submissions/{timestamp}.csv", index=False)
