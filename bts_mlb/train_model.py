#-- base packages
import os
import sys
#import lightgbm




#-- Pypi stats models
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
#import statsmodels.api as sm
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix#, plot_roc_curve
from sklearn.pipeline import Pipeline
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import (train_test_split,
                                     cross_validate,
                                     GridSearchCV
                                    )
from sklearn.metrics import classification_report
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import PrecisionRecallDisplay

def logistic(x_train, y_train):

    model = LogisticRegression(C=1.0, class_weight=None, dual=False, fit_intercept=True,
                   intercept_scaling=1, l1_ratio=None, max_iter=100,
                   n_jobs=None, penalty='l1',
                   random_state=0, solver='liblinear', tol=0.0001, verbose=0,
                   warm_start=False)

    model.fit(x_train, y_train)
    '''
    scores_out = model.score(test[inputs], test['hit_ind'])

    y_preds = model.predict(test[inputs])

    print(str(round(scores_out*100)) + '%' + ' accuracury')

    print(confusion_matrix(y_test, y_preds))
    l1_scores = pd.DataFrame({'classes': inputs_df.columns})
    l1_scores['coeficients'] = model.coef_[0]
    #l1_scores['l1_ratios'] = model.l1_ratios_
    #l1_scores.to_csv('./l1_scores.csv', index=False)
    '''
    return model#, modeling_data[inputs], l1_scores

def lgbm(x_train, y_train, x_test, y_test):
    categorical_features = [c for c, col in enumerate(x_train.columns) if 'cat' in col]
    train_data = lightgbm.Dataset(x_train, label=y_train, categorical_feature=categorical_features)
    test_data = lightgbm.Dataset(x_test, label=y_test)

    parameters = {
    'task': 'train',
    'application': 'binary',
    'objective': 'binary',
    'metric': 'recall',
    'is_unbalance': 'true',
    'boosting': 'gbdt',
    'num_leaves': 20,
    'feature_fraction': 0.5,
    'bagging_fraction': 0.5,
    'bagging_freq': 20,
    'learning_rate': 0.05,
    'verbose': 1
    }

    model = lightgbm.train(parameters,
                       train_data,
                       valid_sets=test_data,
                       num_boost_round=3000,
                       early_stopping_rounds=100)
    return model



def random_forrest():

    inputs = [

        'rp_BA', 'rp_AB_div_PA', 'ytd_BA', 'ytd_AB_div_PA', 'rp_BA_sp',
        'rp_AB_div_PA_sp', 'ytd_BA_sp', 'ytd_AB_div_PA_sp', 'Bot',
        'L-L', 'L-R', 'R-L',
        'rp_hits_var', 'ytd_hits_var',
        'match_year_PAs', 'match_year_BA', 'match_year_AB_div_PA',
        'match_career_PAs', 'match_career_BA', 'match_career_AB_div_PA'
    ]

    pipe = Pipeline([
        ('scale', MinMaxScaler()),
        ('m', RandomForestClassifier())
    ])
    param_grid = {
        'm__n_estimators': [30, 40, 50], # num trees
        'm__max_depth': [14, 22, 30], #max depth of trees
        'm__min_samples_split': [8, 20, 100], #min samples per leaf
        'm__max_features': [5, 10, 15], # num features
        'm__random_state': [33],
    }
    grid = GridSearchCV(pipe,
                        param_grid,
                        scoring='f1_weighted',
                        n_jobs=-1,
                        cv=5
                       )
    grid.fit(train[inputs], train['hit_ind'])
    model = grid.best_estimator_
    cv_results = cross_validate(model,
                                train[inputs],
                                train['hit_ind'],
                                cv=10,
                                n_jobs=-1,
                                scoring='f1_weighted',
                                return_train_score=True
                               )
    cv_results = pd.DataFrame(cv_results)
    # %% validate
    scores_out = model.score(test[inputs], test['hit_ind'])
    y_preds = model.predict(test[inputs])
    y_pred_probs = model.predict_proba(test[inputs])

    prec, recall, thresholds = precision_recall_curve(test['hit_ind'], y_pred_probs[:,1])

    pr_display = PrecisionRecallDisplay(precision=prec, recall=recall).plot()
    plt.show()

    print(str(round(scores_out*100)) + '%' + ' accuracury')
    print(confusion_matrix(test['hit_ind'], y_preds))

    return model, modeling_data[inputs]
