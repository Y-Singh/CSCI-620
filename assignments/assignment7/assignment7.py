import pandas as pd
import numpy as np
from jproperties import Properties
import os

class DataCleaning:
    def __init__(self, path) -> None:
        self.path = path
        self.logger = []

        config = Properties()
        with open(path + '\\config.properties', 'rb') as config_file:
            config.load(config_file)
        self.data_path = config.get("DataPath").data
        
        
        
if __name__ == "__main__":
    path = os.path.dirname(os.path.abspath(__file__))