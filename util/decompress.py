# Import common tables

# from common import
import os
import py7zr
from common import *

KAGGLE_FOLDER = ''

try:
    KAGGLE_FOLDER = os.environ['KAGGLE_FOLDER']
except:
    print('No env variable for KAGGLE_FOLDER, setting manually', os.environ)
    KAGGLE_FOLDER = input('Enter KAGGLE_FOLDER: ')

COMPRESSED_FILES_EXT = 'csv.7z'
DATABASE_FILES_MAPPER = {
    TABLE_TRANSACTIONS: ['transactions', 'transactions_v2'],
    TABLE_USER_LOGS: ['user_logs', 'user_logs_v2'],
    TABLE_MEMBERS: ['members_v3'],
}


def load_data():
    DECOMPRESS_FOLDER = 'decompressed'

    # Create decompress folder if not exists
    if not os.path.exists(DECOMPRESS_FOLDER):
        os.makedirs(DECOMPRESS_FOLDER)

    # Get DATABASE_FILES_MAPPER keys
    db_tables = list(DATABASE_FILES_MAPPER.keys())

    # Load data from compressed files
    for db_table in db_tables:
        files = DATABASE_FILES_MAPPER[db_table]
        print('Loading data from compressed files...' + db_table)
        for file in files:
            try:
                compressed_file = f'{KAGGLE_FOLDER}/{file}.{COMPRESSED_FILES_EXT}'
                print(compressed_file)
                py7zr.unpack_7zarchive(compressed_file, DECOMPRESS_FOLDER)

            except Exception as e:
                print(e)
                continue

if __name__ == '__main__':
    load_data()
