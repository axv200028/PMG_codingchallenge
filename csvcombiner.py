# -*- coding: utf-8 -*-
"""
Created on Sun Apr 10 01:42:23 2022

@author: akhil
"""

import pandas as pd
import sys
import os

class csvcombiner:
    
    def merge_files(self, argv: list):
    """
    This function combines rows of given csv files path through command line arguments and also adds filename column to indicate the row belongs to which file
    chunksize attribute helps to set memory limits for large datasets
    """
        
        datasize = 10**6
        dataframes = []
        
        if self.validate_file(argv):
            filelist = argv[1:]
            
            for file_path in filelist:
                for df in pd.read_csv(file_path, chunksize = datasize):
                    df['filename'] = os.path.basename(file_path)
                    dataframes.append(df)
                    
            df_all = pd.concat(dataframes)
            df_all.to_csv('combined.csv', index = False, chunksize = datasize)
            print(df_all)
            
        else:
            return
        
    @staticmethod
    def validate_file(argv):
    """
    This function checks if the command line arguments are valid and checks if files/directory are not found or empty
    """
        
        if len(argv) <= 1:
            print("No file paths given as arguments. Run code as: \n" + "python ./csvcombiner.py ./fixtures/accessories.csv ./fixtures/clothing.csv > combined.csv")
            return False
        
        files = argv[1:]
        
        for file in files:
            if not os.path.exists(file):
                print("No file or directory found: " + file)
                return False
            if os.stat(file).st_size == 0:
                print("The file is empty: " + file)
                return False
        return True
    
    
    

def main():
    csvcombiner().merge_files(sys.argv)
    
if __name__ == '__main__':
    main()
    