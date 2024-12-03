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
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

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

def writeDfToBq_with_merging(data:pd.DataFrame, project_id:str, dataset_id:str, table_id:str, job_id_prefix:str, cols_to_check:list=[], cols_to_update:list=[]) -> None:
    """Sends a pandas DataFrame to a BigQuery table with automatic schema handling and data merging.

    Args:
        data (pd.DataFrame): The pandas DataFrame to be loaded into BigQuery.
        project_id (str): The ID of your GCP project where the BigQuery dataset resides.
        dataset_id (str): The ID of the BigQuery dataset containing the target table.
        table_id (str): The ID of the BigQuery table to be loaded with the data.
        job_id_prefix (str): A prefix for job IDs to avoid naming conflicts.

    Raises:
        Exception: An exception with details on errors encountered during the process.
    """
    print("Creating the Bigquery client")
    bq_client = bigquery.Client(project=project_id)
    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")
    temp_table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}_temptable")
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE # if the table exists, overwrite it

    # Schema handling (automatic or based on existing table)
    try:
        # Attempt to get the existing production table schema
        table_schema = bq_client.get_table(table).schema 
        
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
    load_job = bq_client.load_table_from_dataframe(
        dataframe = data,
        destination = temp_table,
        job_id = f"{job_id_prefix}_temptable_{pd.to_datetime('now').strftime('%Y%m%d%H%M%S')}",
        job_config = job_config,
        project = project_id
    )
    print(f"Load job creado con el siguiente id: {load_job.job_id}.")

    # Loading data as a temp table
    try:
        output = load_job.result()
        print(f"Load job {load_job.job_id} ejecutado. Output: {output}.")
    except Exception as e:
        raise Exception(f"Se han detectado {len(load_job.errors)} errores durante la ejecución de {load_job.job_id}:\n{[err for err in load_job.errors]}")

    # Merging the temp table into the target table
    cols_to_check = cols_to_check if len(cols_to_check) > 0 else data.select_dtypes(exclude=['float','int']).columns
    cols_to_update = data.select_dtypes(include=['float','int']).columns

    NL = '\n' # new line for f-strings
    merge_query = f"""
    BEGIN
        BEGIN TRANSACTION;
            MERGE INTO `{project_id}.{dataset_id}.{table_id}` AS target
            USING `{project_id}.{dataset_id}.{table_id}_temptable` AS source
            ON {f'{NL}AND '.join([f'target.{to_check} = source.{to_check}' for to_check in cols_to_check])}
            WHEN MATCHED THEN
            UPDATE SET
                {f',{NL}'.join([f'target.{to_update} = source.{to_update}' for to_update in cols_to_update])}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join([*cols_to_check,*cols_to_update])})
                VALUES ({', '.join([f'source.{element}' for element in [*cols_to_check,*cols_to_update]])});

        COMMIT TRANSACTION;
        
        EXCEPTION WHEN ERROR THEN
            SELECT @@error.message;
            ROLLBACK TRANSACTION;
    END;"""
    
    merge_job = bq_client.query(query=merge_query, job_id=f"{job_id_prefix}_merge_data_{pd.to_datetime('now').strftime('%Y%m%d%H%M%S')}")
    try:
        merge_output = merge_job.result()
        print(f"Merge job {merge_job.job_id} executed. Output: {merge_output}.")
    except Exception as e:
        raise Exception(f"Se han detectado {len(merge_job.errors)} errores durante la ejecución de {merge_job.job_id}:\n{[err for err in merge_job.errors]}")
    
    # Delete the temptable after a correct merging
    delete_job = bq_client.query(
        query=f"DROP TABLE `{project_id}.{dataset_id}.{table_id}_temptable`;",
        job_id=f"{job_id_prefix}_delete_temp_data_{pd.to_datetime('now').strftime('%Y%m%d%H%M%S')}"
    )
    try:
        delete_output = delete_job.result()
        print(f"Merge job {delete_job.job_id} executed. Output: {delete_output}.")
    except Exception as e:
        raise Exception(f"Se han detectado {len(delete_job.errors)} errores durante la ejecución de {delete_job.job_id}:\n{[err for err in delete_job.errors]}")
    
    return load_job, merge_job, delete_job