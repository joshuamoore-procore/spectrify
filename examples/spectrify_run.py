"""
This file demonstrates how you can customize/extend the TableTransformer class.
Running this script will:
 - Export previous week's data
 - Convert previous week's data to Parquet
 - Add a partition containing previous week's data to an existing Spectrum table, keyed on date
"""
from datetime import datetime, timedelta
from getpass import getpass
from time import monotonic
import configparser

import sqlalchemy as sa
from spectrify.utils.s3 import SimpleS3Config
from spectrify.transform import TableTransformer
from spectrify.export import RedshiftDataExporter
from spectrify.create import SpectrumTableCreator

section_name = "target_redshift"
config = configparser.ConfigParser()
config.read("/users/joshmoore/Documents/AirflowTest2/redshift_config.cfg")
target_dbname = config.get(section_name, "dbname")
target_user = config.get(section_name, "user")
target_password = config.get(section_name, "password")
target_host = config.get(section_name, "host")
target_port = config.get(section_name, "port")

# csv_path_template = 's3://{csv_bucket_name}/{prefix}/{table_name}/csv/{start.year}/{start.month:02d}/{start.day:02d}/'
# spectrum_path_template = '''s3://{parquet_bucket_name}/{prefix}/{table_name}/parquet/year={start.year}\
# /month={start.month:02d}/day={start.day:02d}/'''
csv_path_template = 's3://{csv_bucket_name}/{csv_prefix}/{source_table}/csv/{start.year}/{start.month:02d}/{start.day:02d}/'
spectrum_path_template = '''s3://{parquet_bucket_name}/{parquet_prefix}/{source_table}/parquet/year={start.year}\
/month={start.month:02d}/day={start.day:02d}/'''


# csv_path_template = 's3://procoreit-analytic-events-transformed/local_dev/transformed_events/csv/{start.year}\
# /{start.month:02d}/{start.day:02d}/'
# spectrum_path_template = '''s3://procoreit-analytic-events-transformed/local_dev/transformed_events/parquet\
# /year={start.year}/month={start.month:02d}/day={start.day:02d}/'''


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


class WeeklyDataTransformer(TableTransformer):
    """The TableTransformer does 3 things:
      - Export Redshift data to CSV
      - Convert CSV to Parquet
      - Create a Spectrum table from Parquet files

    This subclass overrides the default behavior in the following ways:
      - Exports only last week of data (via WeeklyDataExporter)
      - Adds a partition instead of creating new table (via SpectrumPartitionCreator)
    """

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        super().__init__(*args, **kwargs)

    def export_redshift_table(self):
        """Overrides the export behavior to only export the last week's data"""
        exporter = WeeklyDataExporter(
            self.engine,
            self.s3_config,
            start_date=self.start_date,
            end_date=self.end_date
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


class WeeklyDataExporter(RedshiftDataExporter):
    """This class overrides the export query in the following ways:
       - Exports only records with timestamp_col between start_date and end_date
       - Features a smaller MAXFILESIZE (256MB)
    """
    UNLOAD_QUERY = """
    UNLOAD ($$
      SELECT
        *
    FROM
        {table_name}
    --WHERE
        --timestamp >= '2018-07-01 00:00:00'
        --AND timestamp < '2018-07-02 00:00:00'

        --FROM {table_name}
      WHERE timestamp >= '{start_date}' AND timestamp < '{end_date}'

    $$)
    TO '%(s3_path)s'
    CREDENTIALS '%(credentials)s'
    ESCAPE MANIFEST GZIP ALLOWOVERWRITE
    MAXFILESIZE 256 MB;
    """

    def __init__(self, *args, **kwargs):
        self.start_date = kwargs.pop('start_date')
        self.end_date = kwargs.pop('end_date')
        super().__init__(*args, **kwargs)

    def get_query(self, table_name):
        return self.UNLOAD_QUERY.format(
            table_name=table_name,
            start_date=self.start_date,
            end_date=self.end_date,
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


def spectrify_last_week(start_date
                        , end_date
                        , source_schema
                        , source_table
                        , table_name
                        , spectrum_schema
                        , spectrum_table
                        , csv_bucket_name
                        , csv_prefix
                        , parquet_bucket_name
                        , parquet_prefix
                        ):
    # end_date = datetime.utcnow().date()
    # start_date = end_date - timedelta(days=7)

    # Replace with your table names (or pass in as parameters)
    # source_table = 'procore_analytic_events.transformed_events'
    # dest_table = 'analytic_events_spectrum'
    # spectrum_schema = 'analytic_events'

    sa_engine = get_redshift_engine(database=target_dbname
                                    , host=target_host
                                    , user=target_user
                                    , password=target_password
                                    , port=target_port)

    # Construct a S3Config object with the source CSV folder and
    # destination Spectrum/Parquet folder on S3.
    csv_path = csv_path_template.format(start=start_date
                                        , source_table=source_table
                                        , csv_bucket_name=csv_bucket_name
                                        , csv_prefix=csv_prefix)
    spectrum_path = spectrum_path_template.format(start=start_date
                                                  , source_table=source_table
                                                  , parquet_bucket_name=parquet_bucket_name
                                                  , parquet_prefix=parquet_prefix)
    s3_config = SimpleS3Config(csv_path, spectrum_path)

    transformer = WeeklyDataTransformer(
        sa_engine, table_name, s3_config, spectrum_schema, spectrum_table,
        start_date=start_date, end_date=end_date)
    transformer.transform()


start = '2018-08-01 00:00:00.000'
start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S.%f')
end = '2018-08-02 00:00:00.000'
end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S.%f')
source_schema = 'procore_analytic_events'
source_table = 'transformed_events'
table_name = source_schema + "." + source_table
timestamp_col = 'timestamp'
spectrum_schema = 'analytic_events'
spectrum_table = 'analytic_events_spectrum'
csv_bucket_name = 'procoreit-analytic-events-transformed'
csv_prefix = 'prod'
parquet_bucket_name = 'procoreit-analytic-events-transformed'
parquet_prefix = 'prod'


def main():
    run_start_time = monotonic()
    spectrify_last_week(start_date
                        , end_date
                        , source_schema
                        , source_table
                        , table_name
                        , spectrum_schema
                        , spectrum_table
                        , csv_bucket_name
                        , csv_prefix
                        , parquet_bucket_name
                        , parquet_prefix)
    run_end_time = monotonic()
    print('Spectrify operation completed in {} seconds'.format(run_end_time - run_start_time))


if __name__ == '__main__':
    main()
