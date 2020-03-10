import ETL_manageFiles as mf
import ETL_computeValues as cv


def populate_tables(df):
        '''This function takes a df with the data to insert in the LIVE table
        and loops through the values performing a ON DUPLICATE KEY UPDATE'''

        query = """INSERT INTO table (id, name, age) VALUES(%s, %s, %s)
        ON DUPLICATE KEY UPDATE name=%s, age=%s"""
        #engine.execute(query, (df.id[i], df.name[i], df.age[i], df.name[i], df$



        return


print(cv.compute(mf.file_processer("./raw_data/")))
