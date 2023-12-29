from config import KobisConfig

kobis_config = KobisConfig()

def get_raw_dir_path(dir_name):
    storage_loc = kobis_config.data
    return f"{storage_loc}/{dir_name}"

def get_raw_file_path(dir_name, file_name):
    dir_path = get_raw_dir_path(dir_name)
    return f"{dir_path}/{file_name}.csv"