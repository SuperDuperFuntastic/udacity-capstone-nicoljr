import os
from pathlib import Path
from config.definitions import ROOT_DIR

def prepare_bicycle_metadata():
    '''
    Walks through the data folder and parses information from its contents
    
    Returns:
        bicycle_metadata (list): a collection of metadata dictionaries
    '''
    data_folder = os.path.join(ROOT_DIR, 'data/bicycle_counters')
    bicycle_metadata = []
    for root, dir, files in os.walk(data_folder):
        for file in files:
            file_path = Path(os.path.join(root, file))
            city = file_path.parent.name
            state = file_path.parent.parent.name
            country = file_path.parent.parent.parent.name
            bicycle_dict = {"file_path": file_path,
                            "city": city, "state": state,
                            "country": country}
            bicycle_metadata.append(bicycle_dict)
    return bicycle_metadata

def main():
    bicycle_metadata = prepare_bicycle_metadata()
    for item in bicycle_metadata:
        print(item)
    

if __name__ == '__main__':
    main()