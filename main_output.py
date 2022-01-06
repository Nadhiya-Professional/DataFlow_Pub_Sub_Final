import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
from apache_beam.io.gcp.internal.clients import bigquery

if __name__ == '__main__':
    table_spec3 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='n_mathialagan_proj_1',
        tableId='bb')
    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'nullable'},
            {'name': 'last_name', 'type': 'string', 'mode': 'nullable'}
            ]
    }
    pipeline_options = PipelineOptions(region="us-central1",temp_location="gs://york_temp_files",project="york-cdf-start",job_name="dataflow-nadhiya11")
    
    with beam.Pipeline(options=pipeline_options,runner="DataflowRunner") as pipeline:
        output = pipeline | "Read from table" >> beam.io.ReadFromBigQuery(query="select table1.order_id,table2.last_name from  york-cdf-start.bigquerypython.bqtable1 as table1 "
                                                                                "join york-cdf-start.bigquerypython.bqtable4 as table2 on table1.order_id = table2.order_id",project ="york-cdf-start",use_standard_sql=True)

        output | "Write to bigquery" >> beam.io.WriteToBigQuery(
            table_spec3,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    print("done")    

     
