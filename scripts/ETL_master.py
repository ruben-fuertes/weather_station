import ETL_extract as extr
import ETL_transform as tran
import ETL_load as load

df = tran.compute(extr.file_processer("./raw_data/"))

load.load(df)

