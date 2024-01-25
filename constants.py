# Sort of like environment variables, just not secret

import os

compressed_data_path = 'regions_without_service.zip'
data_dir = os.path.splitext(os.path.basename(compressed_data_path))[0]