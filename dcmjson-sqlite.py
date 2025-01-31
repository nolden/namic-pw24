#!/usr/bin/env python

"""Populate SQLite with DICOM metadata.

This code recursively crawls a directory and populates a SQLite database with JSON representations of
the DICOM files found.

Warning: highly experimental code, use at your own risk!

"""

import pydicom
import os
import sys
import sqlite3
import json

def bulk_data_handler(data_element):
    # we are not interested in any bulk data
    return None

crawl_directory = sys.argv[2]
db_path = sys.argv[1]

# Connect to the database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS dicom_files (
    filename TEXT PRIMARY KEY NOT NULL,
    jsonb_data BLOB
)
''')

conn.commit()

# iterate over crawl directory, and insert json dumps of all files to the database
for root,dirs,files in os.walk(crawl_directory):
    for file_name in files:
        dicom_file_path = os.path.join(root,file_name)
        json_data = ""
        
        # check if file is already in the database
        cursor.execute("SELECT filename FROM dicom_files WHERE filename = ?", (dicom_file_path,))
        data=cursor.fetchone()
        
        if data:
            continue

        try:
            ds = pydicom.dcmread(dicom_file_path)
            json_data = ds.to_json_dict(bulk_data_element_handler=bulk_data_handler)
            
            # print(json_data)

        except pydicom.errors.InvalidDicomError:
            print("Skipped %s", dicom_file_path)
            continue
                     
        cursor.execute('''
        INSERT INTO dicom_files (filename, jsonb_data )
        VALUES ( ? , jsonb( ? ) )
        ''', (dicom_file_path, json.dumps(json_data) ) )
        
        conn.commit()
                
        print(f"Inserted DICOM file: {dicom_file_path}")
        
conn.close()
