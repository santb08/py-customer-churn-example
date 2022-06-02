# Kaggle Data Setup
Let's setup a virtual env using the following commands

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
sh decompress.sh
```

Once the decompressing is completely finished we'll clean up a bit this output folder by running the following commands

```
mv decompressed/data/**/** /decompressed
rm -rf decompressed/data
```

When I decompressed that data I noticed that `data` folder, so I thought it was a good idea to have our decompressed output folder completely clean an plain.

# DB Setup

Once we've decompressed our data we can proceed to run