
from __future__ import print_function

import os
from string import Template
import subprocess
import numpy as np
import pandas as pd
import psycopg2
from cdilinker.config.config import config
from sqlalchemy import create_engine
from cdilinker.plugins.field_category import FieldCategory

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


CHUNK_SIZE = 100000
MAX_PAIRS_SIZE = 5000000


MATCHED_FILE_NAME = 'matched_temp.csv'
STEP_MATCH_FILENAME = 'step_matched.csv'

LINKING_RELATIONSHIPS = (
    ('1T1', 'One to One'),
    ('1TM', 'One to Many'),
    ('MT1', 'Many to One'),
)


COLUMN_TYPES = {
    "VARCHAR": object,
    "BOOLEAN": np.bool_,
    "REAL": np.float64,
    "INTEGER": np.int64,
    "CHAR": object,
    "TEXT": object,

}


LINKING_METHODS = {
    'DTR': 'Deterministic',
    'PRB': 'Probabilistic',
}


FIELD_CATEGORIES = [field_cat() for field_cat in FieldCategory.plugins]

# Read the database connection parameters.
con_params = config.get_section('postgres')

engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}/{3}'.format(
    con_params.get('user'),
    con_params.get('password'),
    con_params.get('host'),
    con_params.get('database')))


def _save_pairs(file_path, data, append=False):
    data.replace(np.nan, '', regex=True)
    if not append:
        data.to_csv(file_path)
    else:
        with open(file_path, 'a') as f:
            data.to_csv(f, header=False)


def connect():

    """
    Connects to PostgreSQL database server using the parameters specified in
    the postgres section of the config file.
    :return:
    """

    con = None

    try:
        # Read the database connection parameters.
        con_params = config.get_section('postgres')

        con = psycopg2.connect(**con_params)

    except (Exception, psycopg2.DatabaseError) as err:
        print (err)

    return con


def get_max_cols_len(filepath, columns, data_types):

    '''
    Sacns the entire csv file and calculate the longest string length per column.
    :param filepath: Path(url) to csv file.
    :param columns: List of columns to be scanned.
    :param data_types: Data type of columns
    :return: The maximum length of strings in each column
    '''

    # Convert PostgreSQL data types to pandas data types
    d_types = {col:COLUMN_TYPES[col_type] for (col, col_type) in data_types.items()}

    # Read csv file chunk by chunk and calculate the max column length for each column.
    reader = pd.read_csv(filepath,
                         usecols=columns,
                         dtype=d_types,
                         skipinitialspace=True,
                         chunksize=CHUNK_SIZE)

    lengths = [chunk.apply(lambda x : np.nan if x.dtype != 'object' else x.str.len().max()) for chunk in reader]
    lengths = pd.DataFrame(lengths)
    return lengths.max().dropna().astype(np.int64).to_dict()




def import_data_to_hdf(datastore, table_name, filepath, columns, index_col, data_types, cols_width):

    """
    Imports a csv file into a HDF5 data store as a table.
    :param datastore: HDF5 data store name.
    :param table_name: The name of table to be created.
    :param filepath: CSV file path(url).
    :param columns: CSV data columns.
    :param data_types: Columns data types.
    :param cols_width: Max length of data columns.
    :return:
    """

    # open data store
    data_store = pd.HDFStore(datastore)

    # Convert PostgreSQL data types to pandas data types
    d_types = {col: COLUMN_TYPES[col_type] for (col, col_type) in data_types.items()}

    # Read csv file chunk by chunk
    reader = pd.read_csv(filepath,
                         index_col=index_col,
                         usecols=columns,
                         dtype=d_types,
                         skipinitialspace=True,
                         chunksize=CHUNK_SIZE)

    # Create table and add each chunk of data into it.
    for chunk in reader:
        data_store.append(table_name, chunk, format='table', min_itemsize=cols_width)

    # Save the table into data store and close it.
    data_store.close()


def import_csv_to_postgres(filepath, table_name, columns, index_col, data_types, cols_width):

    """
    Creates a PostgreSQL table from a csv file.
    :param filepath: Path to the csv file(including the file name).
    :param table_name: Name of the table to be created.
    :param columns: CSV header columns.
    :param index_col: Primary key indicator column.
    :param data_types: Data types of columns.
    :param cols_width: Max length of character string columns.
    :return:
    """

    # Get database connection
    #con = connect()

    #cur = con.cursor()

    # Prepare columns definition and create table sql command.
    length_str = {col: '({0})'.format(length) for (col, length) in cols_width.items()}

    columns_def = ', '.join('{0} {1}{2}'.format(col, data_types[col], length_str.get(col, '')) for col in columns)
    columns_def += ", PRIMARY KEY ({0})".format(index_col)

    create_table_sql = "CREATE TABLE {0}({1})".format(table_name, columns_def)

    with engine.begin() as con:
        con.execute("DROP TABLE IF EXISTS {0}".format(table_name))
        con.execute(create_table_sql)

        # Read csv file chunk by chunk and insert it into database table

        # Convert PostgreSQL data types to pandas data types
        d_types = {col: COLUMN_TYPES[col_type] for (col, col_type) in data_types.items()}

        reader = pd.read_csv(filepath,
                             index_col=index_col,
                             usecols=columns,
                             dtype=d_types,
                             skipinitialspace=True,
                             chunksize=CHUNK_SIZE)

        # Create table and add each chunk of data into it.

        for chunk in reader:
            chunk.columns = map(str.lower, chunk.columns)
            chunk.index.rename(index_col.lower(), inplace=True)
            chunk.to_sql(table_name, con, if_exists='append')



def import_project_data(project):

    if project['datasets'] and len(project['datasets']) > 0:
        dataset = project['datasets'][0]

        dtypes = dataset["data_types"]
        usecols = dataset['columns']

        dataset_url = dataset['url']

        cols_width = get_max_cols_len(dataset_url, usecols, dtypes)

        import_csv_to_postgres(dataset_url, 'left_data', usecols, dataset['index_field'], dtypes, cols_width)


def import_project_data_to_hdf5(project):
    data_store = pd.HDFStore('data_link.h5', mode='w')

    data_store.close()

    if project['datasets'] and len(project['datasets']) > 0:
        dataset = project['datasets'][0]

        dtypes = dataset["data_types"]
        usecols = dataset['columns']

        dataset_url = dataset['url']

        cols_width = get_max_cols_len(dataset_url, usecols, dtypes)

        index_col = dataset['index_field']
        cols_width['index'] = cols_width.pop(index_col)

        import_data_to_hdf('data_link.h5', 'left_data', dataset_url, usecols, index_col, dtypes, cols_width)




def sort_csv(filename, appendfile, cols, types):

    template_file = os.path.dirname(__file__) + '/sort_script_template.txt'
    template_script_file = open(template_file)
    sort_script = Template(template_script_file.read())
    import csv
    header = []
    with open(filename, 'r') as in_file:
        reader = csv.reader(in_file)
        header = next(reader)

    col_index = {key: index + 1 for index, key in enumerate(header)}

    bash_str = 'tail -n +2 {0} | split -l 10000 - file_chunk_'.format(filename)
    sort_cols = []
    for col in cols:
        sort_cols.append('-k {0},{0}{1}'.format(col_index[col], 'n' if types[col] == 'numeric' else ''))

    sort_cols = ' '.join(sort_cols)

    params = {
        'filename': filename,
        'sort_cols': sort_cols,
        'chunksize': CHUNK_SIZE,
        'appendfile': appendfile
    }

    sort_script = sort_script.substitute(params)
    script_filename = os.path.join(os.path.dirname(__file__), 'sort_script.sh')

    with open(script_filename, 'w') as script_file:
        script_file.write(sort_script)
    os.chmod(script_filename, 0o777)
    subprocess.call([script_filename])
    os.remove(script_filename)



