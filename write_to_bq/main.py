from google.cloud import bigquery
import pandas as pd

def writeDfToBq(data:pd.DataFrame, project_id:str, dataset_id:str, table_id:str, job_id_prefix:str) -> bigquery.LoadJob:
    """
    Writes a pandas DataFrame to a BigQuery table.

    This function takes a DataFrame, project ID, dataset ID, table ID, and a job ID prefix,
    and configures a BigQuery load job to insert the DataFrame data into the specified table.
    It handles schema management and job configuration for efficient data loading.

    Args:
        data (pd.DataFrame): The pandas DataFrame to be written to BigQuery.
        project_id (str): The ID of the BigQuery project.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        job_id_prefix (str): A prefix for the load job ID (timestamp will be appended).

    Returns:
        bigquery.LoadJob: The created BigQuery load job object.
    """

    # BigQuery client and table reference
    client = bigquery.Client(project=project_id)
    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")

    # Load job configuration
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED # Si la tabla no existe, se crea
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND # Si la tabla existe, se hace append
    # job_config.time_partitioning = bigquery.table.TimePartitioning(type_='DAY') # Particionamiento de la tabla en base al día de subida

    # Schema handling (automatic or based on existing table)
    try:
        # Attempt to get the existing table schema
        table_schema = client.get_table(table).schema
        
        # Remove fields from the schema that are not present in the DataFrame
        fields_to_remove = [field for field in table_schema if field.name not in data.columns]
        for field in fields_to_remove:
            table_schema.remove(field)
        job_config.schema = table_schema
        job_config.autodetect = False # Use the provided schema
    except:
        job_config.autodetect = True
        print('No se ha podido recuperar el esquema de la tabla. Es posible que la tabla no exista.')
    
    # Create the load job
    load_job = client.load_table_from_dataframe(
        dataframe = data,
        destination = table,
        job_id = f"{job_id_prefix}_{pd.to_datetime('now').strftime('%Y%m%d%H%M%S')}",
        job_config = job_config,
        project = project_id
    )
    print(f"Load job creado con el siguiente id: {load_job.job_id}.")
    return load_job
