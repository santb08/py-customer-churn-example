# Kaggle Data Setup
Download kaggle data by using [Kaggle API](https://github.com/Kaggle/kaggle-api).

```
kaggle competitions download -c kkbox-churn-prediction-challenge
```

Or downloading data manually from [Kaggle](https://www.kaggle.com/competitions/kkbox-churn-prediction-challenge/data)

Then, we'll setup a virtual environment for Python using the following commands

```
python -m venv ./venv
source ./source/bin/activate
```

Install our dependencies with

```
pip install -r requirements.txt
```

Then we'll start running our `decompress.sh` file for setup our data downloaded in Kaggle

```
sh scripts/decompress.sh
```

Once the decompressing is completely finished we'll clean up a bit this output folder by running the following commands

```
mv decompressed/data/churn_comp_refresh/** decompressed
rm -rf decompressed/data
```

When I decompressed that data I noticed that `data` folder, so I thought it was a good idea to have our decompressed output folder completely clean an plain.

# Setup

Once we've decompressed our data we can proceed to run the next bash script

```
sh scripts/setup.sh
```

This will setup a new database and populate it with the decompressed data csv files. Now we can proceed to predict using the `prediction_engineering.sql` example in Python.

# Prediction Engineering (Python Approach)

Finally we can run our prediction

```
python prediction_engineering.py
```

And see the results in `prediction.csv` new file.