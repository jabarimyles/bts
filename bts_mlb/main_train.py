#-- base packages
import os
import sys
import pickle

#-- pypi packages
import pandas as pd

#-- custom packages
from train_model import logistic
from createModelingData import get_modeling_data
from .gcs_helpers import *


import tempfile
import json

cred_path = "/tmp/service_account.json"
with open(cred_path, "w") as f:
    f.write(os.environ["GOOGLE_CREDENTIALS_JSON"])

# Set the environment variable for Google auth
#os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path



if __name__ == '__main__':
    models_dir = ''
    dup_model_app = '___'

    model_name = "logistic"#sys.argv[1]
    # model_names = [i.split(dup_model_app)[0] for i in os.listdir(models_dir)]
    # conflicts = [i for i in model_names if model_name == i]
    # num_conflicts = len(conflicts)
    # if num_conflicts > 0:
    #     model_name = model_name + dup_model_app + str(num_conflicts + 1)
    #     print('model name already taken... new model renamed to {}'.format(model_name))
    full_model_path = os.path.join(models_dir, model_name)
    #os.mkdir(full_model_path)

    x_train, y_train, x_test, y_test = get_modeling_data()
    write_csv_to_gcs(x_train, 'bts-mlb', 'x_train.csv')
    write_csv_to_gcs(y_train, 'bts-mlb', 'y_train.csv')
    write_csv_to_gcs(x_test, 'bts-mlb', 'x_test.csv')
    write_csv_to_gcs(y_test, 'bts-mlb', 'y_test.csv')


    id_vars = ['game_date', 'game_pk', 'batter', 'starting_pitcher', 'ABs', 'hits', 'hit_ind']
    model = logistic(x_train.drop(id_vars, axis=1), y_train)

    model_name_file = model_name + '.pickle'
    model_pkl_fp = os.path.join(full_model_path, model_name_file)
    upload_pickle_to_gcs('bts-mlb', model_name_file, model)
