from __future__ import absolute_import
import argparse
import logging
import re
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

g_schema = {
    'fields': [{
        'name': 'Year', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'Week', 'type': 'STRING', 'mode': 'REQUIRED'
    }, {
        'name': 'Spot', 'type': 'DECIMAL', 'mode': 'NULLABLE'
    }]
}


class DataIngestion:
    """A helper class which contains the logic to translate the file into
    a format BigQuery will accept."""

    def parse_method(self, string_input):
        """This method translates a single line of comma separated values to a
        dictionary which can be loaded into BigQuery.
        Args:
            string_input: A comma separated list of values in the form of
                state_abbreviation,gender,year,name,count_of_babies,dataset_created_date
                Example string_input: KS,F,1923,Dorothy,654,11/28/2016
        Returns:
            A dict mapping BigQuery column names as keys to the corresponding value
            parsed from string_input. In this example, the data is not transformed, and
            remains in the same format as the CSV.
         """
        logging.debug("DataIngestion.parse_method(...) started")
        data_schema = {
            'fields': [{
                'name': 'Year', 'type': 'STRING', 'mode': 'REQUIRED'
            }, {
                'name': 'Week', 'type': 'STRING', 'mode': 'REQUIRED'
            }, {
                'name': 'Spot', 'type': 'DECIMAL', 'mode': 'NULLABLE'
            }]
        }

        # Strip out carriage return, newline and quote characters.
        values = re.split(";", re.sub('\r\n', '', re.sub('"', '',
                                                         string_input)))
        cols = []
        scehma_fields = data_schema['fields']
        for x in scehma_fields: cols.append(x['name'])
        row = dict(
            zip(cols,
                values))
        logging.debug("DataIngestion.parse_method(...): {}".format(row))
        return row


def run(argv=None, save_main_session=True):
    logging.info("Starting RawImportSweElectricityCertQuotaLevelsHistory")
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://dataflow-sample',
        help='Input file to process.')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    (p

     | 'Read from a File' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
     | 'String To BigQuery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
     | 'Write to BigQuery' >> beam.io.Write(
                # beam.io.BigQuerySink(
                beam.io.WriteToBigQuery(
                    known_args.output,
                    schema=g_schema,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    # Deletes all data in the BigQuery table before writing.
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)))
    p.run().wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
