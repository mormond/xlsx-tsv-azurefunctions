import logging
import pandas as pd
import os
import sys

import azure.functions as func

def main(   inblob: func.InputStream,
            outblob: func.Out[bytes]):

    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {inblob.name}\n"
                 f"Blob Size: {inblob.length} bytes"
                 f"Uri: {inblob.uri}")

    filename = inblob.name
    basefilename, ext = os.path.splitext(filename)

    blob_bytes = inblob.read()

    # Load file into a Pandas dataframe
    exceldf = pd.read_excel(blob_bytes, 'Sheet1', index_col=None)

    # Replace all columns having spaces with underscores
    exceldf.columns = [c.replace(' ', '_') for c in exceldf.columns]

    # Replace all fields having line breaks with space
    df = exceldf.replace('\n', ' ', regex=True)

    #Write dataframe into csv
    result = df.to_csv(sep='\t', encoding='utf-8',  index=False, quotechar='#', line_terminator='\r\n')

    outblob.set(result)
