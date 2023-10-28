import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions


def run_pipeline_bq_to_bq():

    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'YOUR_PROJECT_ID'  # Replace with your actual project ID
    google_cloud_options.job_name = 'kdrama-transformjob'  # Replace with your desired job name
    google_cloud_options.staging_location = 'gs://YOUR_BUCKET/stage_files'  # Replace with your GCS staging bucket
    google_cloud_options.temp_location = 'gs://YOUR_BUCKET/temp_files'  # Replace with your GCS Temporary files bucket


    with beam.Pipeline(options=options) as pipeline:

        table_schema = {
            'fields': [
                {'name': 'Year_of_release', 'type': 'INTEGER'},
                {'name': 'Avg_Eps', 'type': 'FLOAT64'},
                {'name': 'Total_Great_Drama', 'type': 'INTEGER'},
                {'name': 'Total_Excellent_Drama', 'type': 'INTEGER'},
                {'name': 'Total_Exceptional_Drama', 'type': 'INTEGER'}
            ]
        }

        data = pipeline | "Read data from BigQuery" >> beam.io.ReadFromBigQuery(
                                    query='SELECT '
                                            'Year_of_release,'
                                            'ROUND(AVG(Number_of_Episode), 2) AS Avg_Eps,'
                                            'COUNTIF(Rating < 8.5) AS Total_Great_Drama,'
                                            'COUNTIF(Rating >= 8.5 AND Rating < 9.0) AS Total_Excellent_Drama,'
                                            'COUNTIF(Rating >= 9.0) AS Total_Exceptional_Drama '
                                          'FROM '
                                            'YOUR_PROJECT_ID.YOUR_DATASET.kdrama_raw '
                                          'GROUP BY Year_of_release '
                                          'ORDER BY Year_of_release'
                                  ,
                                  use_standard_sql=True)
        _ = (data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                            table='YOUR_PROJECT_ID:YOUR_DATASET.kdrama_dashboard', # Replace with your actual project ID and Dataset Name
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                            )
            )

if __name__ == '__main__':
    run_pipeline_bq_to_bq()