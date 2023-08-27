"""
Trying out prefect's functionality using DuckDB
"""

from datetime import timedelta

import duckdb
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash


def get_connection(db: str):
    return duckdb.connect(db)


def count_distinct(db: str, table: str, column: str):
    conn = get_connection(db)
    query = f"select count(distinct {column}) from {table}"
    result = conn.sql(query).fetchone()[0]
    conn.close()
    return result


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def count_patients(db: str, table: str, id_var: str = 'patient'):
    patient_count = count_distinct(db, table, id_var)
    print(f"# of patients: {patient_count}")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def count_encounters(db: str, table: str, id_var: str = 'encounter'):
    encounter_count = count_distinct(db, table, id_var)
    print(f"# of encounters: {encounter_count}")


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=1))
def count_medications(db: str, table: str, id_var: str = 'medication_encounter'):
    medication_count = count_distinct(db, table, id_var)
    print(f"# of medications: {medication_count}")


@flow(name="DuckDB Stats")
def db_stats(db: str):
    logger = get_run_logger()
    logger.info(f"Getting stats on database {db}")
    count_patients(db, 'stg_patients', 'patient')
    count_encounters(db, 'stg_encounters', 'encounter')
    count_medications(db, 'stg_medications', 'medication_encounter')



if __name__ == "__main__":
    db_stats('../dbt-synthea/synthea.duckdb')
