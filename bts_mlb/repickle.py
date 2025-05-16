# re_pickle_model.py

import pickle
import numpy as np
import sklearn
old_pickle_path = "/Users/jabarimyles/Documents/bts-mlb/prod_model/model2.pickle"         # Replace with your existing file
new_pickle_path = "/Users/jabarimyles/Documents/bts-mlb/prod_model/model.pickle"     # Replace with desired new file name

# Load the original pickled object
with open(old_pickle_path, "rb") as f:
    model = pickle.load(f)

print("Model loaded successfully with numpy version:", np.__version__)

# Save it again using the same numpy version
with open(new_pickle_path, "wb") as f:
    pickle.dump(model, f)

print("Model re-saved successfully at:", new_pickle_path)
