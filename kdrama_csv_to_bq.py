import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions

class RowTransformer(beam.DoFn):
  def __init__(self, delimiter):
    self.delimiter = delimiter

  def process(self, row):
    import csv
    csv_object = csv.reader(row.splitlines(), quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL)
    name, aired_date, year_of_release, original_network, aired_on, number_of_episodes, duration, content_rating,\
    rating, synopsis, genre, tags, director, screenwriter, cast, production_companies, rank = next(csv_object)

    return [{'Name': name, 'Aired_Date':aired_date, 'Year_of_release':year_of_release, 'Original_Network':original_network,
             'Aired_On':aired_on, 'Number_of_Episode':number_of_episodes, 'Duration':duration, 'Content_Rating':content_rating,
             'Rating':rating, 'Synopsis':synopsis, 'Genre':genre, 'Tags':tags, 'Director':director, 'Screenwriter':screenwriter,
             'Cast':cast, 'Production_companies':production_companies, 'Rank':rank }]


def run_pipeline_csv_to_bq():
    table_schema = {
        'fields': [
            {'name': 'Name', 'type': 'STRING'},
            {'name': 'Aired_Date', 'type': 'STRING'},
            {'name': 'Year_of_release', 'type': 'INTEGER'},
            {'name': 'Original_Network', 'type': 'STRING'},
            {'name': 'Aired_On', 'type': 'STRING'},
            {'name': 'Number_of_Episode', 'type': 'INTEGER'},
            {'name': 'Duration', 'type': 'STRING'},
            {'name': 'Content_Rating', 'type': 'STRING'},
            {'name': 'Rating', 'type': 'FLOAT64'},
            {'name': 'Synopsis', 'type': 'STRING'},
            {'name': 'Genre', 'type': 'STRING'},
            {'name': 'Tags', 'type': 'STRING'},
            {'name': 'Director', 'type': 'STRING'},
            {'name': 'Screenwriter', 'type': 'STRING'},
            {'name': 'Cast', 'type': 'STRING'},
            {'name': 'Production_companies', 'type': 'STRING'},
            {'name': 'Rank', 'type': 'STRING'}
        ]
    }

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'YOUR_PROJECT_ID'  # Replace with your actual project ID
    google_cloud_options.job_name = 'kdramajob'  # Replace with your desired job name
    google_cloud_options.staging_location = 'gs://YOUR_BUCKET/stage_files'  # Replace with your GCS staging bucket
    google_cloud_options.temp_location = 'gs://YOUR_BUCKET/temp_files'  # Replace with your GCS Temporary files bucket


    with beam.Pipeline(options=options) as pipeline:
        # Read from the input CSV file
        lines = pipeline | 'ReadFromCSV' >> ReadFromText('gs://YOUR_BUCKET/kdrama.csv', skip_header_lines=1)
        # Parse CSV lines into dictionaries
        parsed_data = (lines
                       | "Transform Data" >> beam.ParDo(RowTransformer(',')))
        _ = (parsed_data
            | 'WriteToBigQuery' >> WriteToBigQuery(
            table='YOUR_PROJECT_ID:_YOUR_DATASET.kdrama_raw', # Replace with your actual project ID and Dataset Name
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
            )

if __name__ == '__main__':
    run_pipeline_csv_to_bq()