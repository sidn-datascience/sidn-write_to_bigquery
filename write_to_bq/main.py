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


def writeDfToBq_with_merging(
    data:pd.DataFrame,
    project_id:str,
    dataset_id:str,
    table_id:str,
    job_id_prefix:str,
    cols_to_check:list=[],
    cols_to_update:list=[]
) -> tuple[bigquery.LoadJob, bigquery.QueryJob, bigquery.QueryJob]:
    """Sends a pandas DataFrame to a BigQuery table, performing schema handling, data loading, and merging with the existing data.

    This function loads data from a pandas DataFrame into a temporary BigQuery table, merges the data with an existing target table,
    and then deletes the temporary table after the merge. The schema is either detected automatically or based on the existing table schema.

    Args:
        data (pd.DataFrame): The pandas DataFrame containing the data to be loaded into BigQuery.
        project_id (str): The ID of the Google Cloud project where the BigQuery dataset resides.
        dataset_id (str): The ID of the BigQuery dataset containing the target table.
        table_id (str): The ID of the BigQuery table to merge the data into.
        job_id_prefix (str): A prefix for the job IDs to avoid conflicts between multiple jobs.
        cols_to_check (list, optional): A list of column names used to match records between the source and target tables. Defaults to columns with non-numeric types in `data`.
        cols_to_update (list, optional): A list of column names to update in the target table if a match is found. Defaults to numeric columns in `data`.

    Returns:
        tuple: A tuple containing three job objects:
            - load_job (bigquery.LoadJob): The job that loads data into the temporary table.
            - merge_job (bigquery.QueryJob): The job that performs the merge operation from the temporary table to the target table.
            - delete_job (bigquery.QueryJob): The job that deletes the temporary table after the merge operation.

    Raises:
        Exception: If any errors occur during the load, merge, or delete operations, an exception is raised with details about the errors.

    Example:
        load_job, merge_job, delete_job = writeDfToBq_with_merging(
            data=df, 
            project_id='my_project', 
            dataset_id='my_dataset', 
            table_id='my_table', 
            job_id_prefix='job_123'
        )
    """
    cols_to_check = cols_to_check if len(cols_to_check) > 0 else data.select_dtypes(exclude=['float','int']).columns
    if len(cols_to_check) == 0:
        raise Exception("No columns to check were provided.")
    
    cols_to_update = cols_to_update if len(cols_to_update) > 0 else data.select_dtypes(include=['float','int']).columns

    print("Creating the Bigquery client")
    bq_client = bigquery.Client(project=project_id)
    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}")
    temp_table = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}_temptable")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE # if the table exists, overwrite it
    )

    # Schema handling (automatic or based on existing table)
    table_schema = None
    try:
        # Attempt to get the existing production table schema
        table_schema = bq_client.get_table(table).schema 
        
        # Remove fields from the schema that are not present in the DataFrame
        fields_to_remove = [field for field in table_schema if field.name not in data.columns]

        for field in fields_to_remove:
            table_schema.remove(field)
        job_config.schema = table_schema
        print(table_schema)
        job_config.autodetect = False # Use the provided schema
        job_config.job_retry = None
    except:
        job_config.autodetect = True
        print('No se ha podido recuperar el esquema de la tabla. Es posible que la tabla no exista.')
    
    # STEP 1: Create the load job
    all_fields = [field.name for field in table_schema] if table_schema else data.columns
    load_job = bq_client.load_table_from_dataframe(
        dataframe = data.loc[:, all_fields],
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

    # STEP 2: Merging the temp table into the target table

    # Preparing the fields' list variables
    temp_table_schema = bq_client.get_table(temp_table).schema
    repeated_fields = [
        field.name for field in temp_table_schema
        if field.mode == "REPEATED"
    ]
    cols_to_update += [col for col in all_fields if col not in cols_to_check+cols_to_update]

    NL = '\n' # new line for f-strings
    on_clause_parts = [f"target.{col} = source.{col}" for col in cols_to_check]
    on_clause_parts+= [f"target.{col} = source.{col}" for col in repeated_fields if col not in cols_to_check]
    on_clause = f"{NL}AND ".join(on_clause_parts)

    merge_query = f"""
    BEGIN
        BEGIN TRANSACTION;
            MERGE INTO `{project_id}.{dataset_id}.{table_id}` AS target
            USING `{project_id}.{dataset_id}.{table_id}_temptable` AS source
            ON {on_clause}
            WHEN MATCHED THEN
            UPDATE SET
                {f', '.join([f'target.{col} = source.{col}' for col in cols_to_update])}
            WHEN NOT MATCHED THEN
                INSERT ROW
                ;
        COMMIT TRANSACTION;
        
        EXCEPTION WHEN ERROR THEN
            SELECT @@error.message;
            ROLLBACK TRANSACTION;
    END;"""
    
    merge_job = bq_client.query(
        query=merge_query, 
        job_id=f"{job_id_prefix}_merge_data_{pd.to_datetime('now').strftime('%Y%m%d%H%M%S')}",
        job_retry=None
    )
    try:
        merge_output = merge_job.result()
        print(f"Merge job {merge_job.job_id} executed. Output: {merge_output}.")
    except Exception as e:
        raise Exception(f"Se han detectado {len(merge_job.errors)} errores durante la ejecución de {merge_job.job_id}:\n{[err for err in merge_job.errors]}")
    
    # STEP 3: Delete the temptable after a correct merging
    delete_job = bq_client.query(
        query=f"DROP TABLE `{project_id}.{dataset_id}.{table_id}_temptable`;",
        job_id=f"{job_id_prefix}_delete_temp_data_{pd.to_datetime('now').strftime('%Y%m%d%H%M%S')}",
        job_retry=None
    )
    try:
        delete_output = delete_job.result()
        print(f"Delete job {delete_job.job_id} executed. Output: {delete_output}.")
    except Exception as e:
        raise Exception(f"Se han detectado {len(delete_job.errors)} errores durante la ejecución de {delete_job.job_id}:\n{[err for err in delete_job.errors]}")
    
    return load_job, merge_job, delete_job
