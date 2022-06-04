#! bin/bash
# Change this folder for the folder name of the downloaded and decompressed file from Keppler
export KAGGLE_FOLDER="kkbox-churn-prediction-challenge"
echo "Decompressing the downloaded file in" $KAGGLE_FOLDER
python util/decompress.py --KAGGLE_FOLDER=$KAGGLE_FOLDER