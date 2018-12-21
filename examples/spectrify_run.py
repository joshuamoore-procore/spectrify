"""
This file demonstrates how you can customize/extend the TableTransformer class.
Running this script will:
 - Export previous week's data
 - Convert previous week's data to Parquet
 - Add a partition containing previous week's data to an existing Spectrum table, keyed on date
"""
from datetime import datetime
from time import monotonic
import configparser
import sys
import json
from pprint import pprint
import click
import sqlalchemy as sa
import logging
from logging.handlers import TimedRotatingFileHandler

# Had to add this line to ensure it was pulling from the modified Spectrify code, rather than the site-packages version.
sys.path.insert(0, '/Users/joshmoore/Documents/GitHub/procore/spectrify')
from spectrify.transform import TableTransformer
from spectrify.export import RedshiftDataExporter
from spectrify.utils.s3 import SimpleS3Config
from spectrify.create import SpectrumTableCreator

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(funcName)s:%(lineno)d — %(message)s")
LOG_FILE = "spectrify_run.log"

def get_console_handler():
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   return console_handler
def get_file_handler():
   file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight')
   file_handler.setFormatter(FORMATTER)
   return file_handler
def get_logger(logger_name):
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.DEBUG) # better to have too much log than not enough
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler())
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger


csv_path_template = '''s3://{csv_bucket_name}/{csv_prefix}/{source_schema}/{source_table}/csv/{start.year}/\
{start.month:02d}/{start.day:02d}/{source_table}_{start.year}{start.month:02d}{start.day:02d}_'''
spectrum_path_template = '''s3://{parquet_bucket_name}/{parquet_prefix}/{source_schema}/{source_table}/parquet/\
year={start.year}/month={start.month:02d}/day={start.day:02d}/'''


def get_redshift_engine(user, password, host, port, database):
    """Helper function for getting a SqlAlechemy engine for Redshift.
    Update with your own configuration settings.
    """
    url = 'redshift+psycopg2://{user}:{passwd}@{host}:{port}/{database}'.format(
        user=user,
        passwd=password,
        host=host,
        port=port,
        database=database,
    )
    return sa.create_engine(url, connect_args={'sslmode': 'prefer'})


class TimedDataTransformer(TableTransformer):
    """The TableTransformer does 3 things:
      - Export Redshift data to CSV
      - Convert CSV to Parquet
      - Create a Spectrum table from Parquet files

    This subclass overrides the default behavior in the following ways:
      - Exports only a specified timeframe of data (via WeeklyDataExporter)
      - Adds a partition instead of creating new table (via SpectrumPartitionCreator)
    """

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        if 'timestamp_col' in kwargs and kwargs['timestamp_col'] is not None:
            self.timestamp_col = kwargs.pop('timestamp_col')
        if 'iam_role' in kwargs:
            if kwargs['iam_role'] is not None:
                self.iam_role = kwargs.pop('iam_role')
        self.export_whole_table = kwargs.pop('export_whole_table')
        super().__init__(*args, **kwargs)

    def export_redshift_table(self):
        """Overrides the export behavior to only export the last week's data"""
        exporter = TimedDataExporter(
            self.engine,
            self.s3_config,
            start_date=self.start_date,
            end_date=self.end_date,
            timestamp_col=self.timestamp_col,
            export_whole_table=self.export_whole_table,
            iam_role=self.iam_role

        )
        exporter.export_to_csv(self.table_name)

    def create_spectrum_table(self):
        """Overrides create behavior to add a new partition to an existing table"""
        creator = SpectrumPartitionCreator(
            self.engine,
            self.spectrum_schema,
            self.spectrum_name,
            self.sa_table,
            self.s3_config,
            start_date=self.start_date,
            end_date=self.end_date
        )
        creator.create()


class TimedDataExporter(RedshiftDataExporter):
    """This class overrides the export query in the following ways:
       - Exports only records with timestamp_col between start_date and end_date
       - Features a smaller MAXFILESIZE (256MB)
    """
    UNLOAD_QUERY = """
    UNLOAD ($$
    SELECT *
    FROM {table_name}
    {where_clause}
    $$)
    TO '%(s3_path)s'
    CREDENTIALS '%(credentials)s'
    {unload_options}
    ;
    """

    UNLOAD_OPTIONS = """ESCAPE MANIFEST GZIP ALLOWOVERWRITE MAXFILESIZE 256 MB"""

    WHERE_CLAUSE_BASE = """WHERE 
    "{timestamp_col}" >= '{start_date}' AND "{timestamp_col}" < '{end_date}'"""

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        self.timestamp_col = kwargs.pop('timestamp_col')
        self.export_whole_table = kwargs.pop('export_whole_table')
        self.iam_role = kwargs.pop('iam_role')
        super().__init__(*args, **kwargs)

    def get_query(self, table_name):
        if self.export_whole_table == 'True':
            query_where_clause = ''
        else:
            query_where_clause = self.WHERE_CLAUSE_BASE.format(
                start_date=self.start_date,
                end_date=self.end_date,
                timestamp_col=self.timestamp_col)
        return self.UNLOAD_QUERY.format(
            table_name=table_name,
            where_clause=query_where_clause,
            unload_options=self.UNLOAD_OPTIONS
        )


class SpectrumPartitionCreator(SpectrumTableCreator):
    """Instead of issuing a CREATE TABLE statement, this subclass creates a
    new partition.
    """
    create_query = """
    ALTER TABLE {spectrum_schema}.{dest_table}
    ADD PARTITION(year='{start.year}',month='{start.month:02d}',day='{start.day:02d}')
    LOCATION '{partition_path}';
    """

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        super().__init__(*args, **kwargs)

    def format_query(self):
        partition_path = self.s3_config.spectrum_dir
        return self.create_query.format(
            spectrum_schema=self.schema_name,
            dest_table=self.table_name,
            start=self.start_date,
            partition_path=partition_path,
        )


def spectrify_run(start
                  , end
                  , source_schema
                  , source_table
                  , table_name
                  , spectrum_schema
                  , spectrum_table
                  , csv_bucket_name
                  , csv_prefix
                  , parquet_bucket_name
                  , parquet_prefix
                  , sa_engine
                  , export_whole_table=True
                  , timestamp_col=None
                  , iam_role=None
                  ):

    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S.%f')

    # Construct a S3Config object with the source CSV folder and
    # destination Spectrum/Parquet folder on S3.
    csv_path = csv_path_template.format(start=start_date
                                        , source_schema=source_schema
                                        , source_table=source_table
                                        , csv_bucket_name=csv_bucket_name
                                        , csv_prefix=csv_prefix)
    spectrum_path = spectrum_path_template.format(start=start_date
                                                  , source_schema=source_schema
                                                  , source_table=source_table
                                                  , parquet_bucket_name=parquet_bucket_name
                                                  , parquet_prefix=parquet_prefix)
    s3_config = SimpleS3Config(csv_path, spectrum_path)

    transformer = TimedDataTransformer(
        sa_engine, table_name, s3_config, spectrum_schema, spectrum_table,
        start_date=start_date, end_date=end_date, timestamp_col=timestamp_col
        , export_whole_table=export_whole_table, iam_role=iam_role
    )
    transformer.transform()


@click.command()
@click.option(
    '--environment',
    '-env',
    default='local_dev',
    help='Environment from SDLC. Values are local_dev, dev, and prod.'
)
@click.option(
    '--export_whole_table',
    '--e',
    default=False,
    help='Boolean for exporting entire source_table. If false, pass in start, end, and timestamp columns to filter.'
)
@click.option(
    '--params',
    '-p',
    type=click.File('r'),
    help='''Configuration file in JSON format containing the parameters to pass in. Values are: start, end, 
    source_schema, source_table, timestamp_col, spectrum_schema, spectrum_table, csv_bucket_name, csv_prefix,
    parquet_bucket_name, parquet_prefix, and iam_role.
    '''
)
def main(environment, export_whole_table, params):
    logger = get_logger("spectrify")

    section_name = "target_redshift"
    config = configparser.ConfigParser()

    if environment == 'local_dev':
        config.read("/users/joshmoore/Documents/AirflowTest2/redshift_config.cfg")
    else:
        config.read("redshift_config.cfg")
    target_dbname = config.get(section_name, "dbname")
    target_user = config.get(section_name, "user")
    target_password = config.get(section_name, "password")
    target_host = config.get(section_name, "host")
    target_port = config.get(section_name, "port")
    if params:
        spectrify_params = json.load(params)
    #pprint(spectrify_params)
    #logger.debug("Params were: {}".format(spectrify_params))

    # Set optional arguments if passed in
    timestamp_col = None
    iam_role = None
    if 'iam_role' in spectrify_params:
        iam_role = spectrify_params['iam_role']
    if 'timestamp_col' in spectrify_params:
        timestamp_col = spectrify_params['timestamp_col']
        if export_whole_table == 'True':
            raise Exception("Parameter export_whole_table set to True, but timestamp_col supplied.")
    # Set csv and parquet prefixes to the environment parameter if it's null
    if spectrify_params['csv_prefix'] is None or spectrify_params['csv_prefix'] == '':
        spectrify_params['csv_prefix'] = environment
    if spectrify_params['parquet_prefix'] is None or spectrify_params['parquet_prefix'] == '':
        spectrify_params['parquet_prefix'] = environment

    sa_engine = get_redshift_engine(database=target_dbname
                                    , host=target_host
                                    , user=target_user
                                    , password=target_password
                                    , port=target_port)
    run_start_time = monotonic()
    logger.debug("Run for {start} to {end} started at {run_start_time}".format(
        start=spectrify_params['start'],
        end=spectrify_params['end'],
        run_start_time=datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f')
    ))
    try:
        spectrify_run(start=spectrify_params['start']
                      , end=spectrify_params['end']
                      , source_schema=spectrify_params['source_schema']
                      , source_table=spectrify_params['source_table']
                      , table_name=(spectrify_params['source_schema']+"."+spectrify_params['source_table'])
                      , spectrum_schema=spectrify_params['spectrum_schema']
                      , spectrum_table=spectrify_params['spectrum_table']
                      , csv_bucket_name=spectrify_params['csv_bucket_name']
                      , csv_prefix=spectrify_params['csv_prefix']
                      , parquet_bucket_name=spectrify_params['parquet_bucket_name']
                      , parquet_prefix=spectrify_params['parquet_prefix']
                      , timestamp_col=timestamp_col
                      , export_whole_table=export_whole_table
                      , sa_engine=sa_engine
                      , iam_role=iam_role
                      )
        run_end_time = monotonic()
        logger.debug("Run for {start} to {end} ended successfully at {run_end_time} and took {duration} seconds.".format(
            start=spectrify_params['start'],
            end=spectrify_params['end'],
            run_end_time=datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f'),
            duration=run_end_time-run_start_time
        ))
    except Exception as e:
        fail_time = monotonic()
        logger.debug("Run for {start} to {end} failed at {fail_time} and took {duration} seconds.\nFailure was:\n {e}".format(
            start=spectrify_params['start'],
            end=spectrify_params['end'],
            fail_time=datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S.%f'),
            duration=fail_time-run_start_time,
            e=e
        ))
    run_end_time = monotonic()
    print('Spectrify operation completed in {} seconds'.format(run_end_time - run_start_time))


if __name__ == '__main__':
    main()
