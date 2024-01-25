import os
import shutil
import zipfile

from constants import compressed_data_path, data_dir

# Delete uncompressed data directory
if os.path.exists(data_dir):
    shutil.rmtree(data_dir)

# Unzip data
with zipfile.ZipFile(compressed_data_path, 'r') as zip_ref:
    zip_ref.extractall()

