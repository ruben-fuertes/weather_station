import ETL_extract as extr
import ETL_transform as tran
import ETL_load as load
import os
import shutil

source = "2020_02_23.csv"
destination = "raw_data/unprocessed/" + source

shutil.copyfile(source, destination)


df = tran.compute(extr.file_processer("./raw_data/"))
print(df)
load.populate_live(df)

load.populate_hour()

