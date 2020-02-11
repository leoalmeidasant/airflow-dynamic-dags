from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator


def mysql_to_s3_loader(dag_id, schedule, default_args, catchup, args):

    config_file = args['config_file']

    # Creating DAG object
    dag = DAG(
        dag_id,
        schedule_interval=schedule,
        default_args=default_args,
        catchup=catchup
    )

    # Creating start and end dummy tasks
    start = DummyOperator(
        task_id='start',
        dag=dag
    )

    end = DummyOperator(
        task_id='end',
        dag=dag
    )

    tables = config_file['tables']

    for table in tables:
        for table, config_dict in table.items():

            extract_data = DummyOperator(
                task_id='{}-data-extraction'.format(table),
                dag=dag
            )

            start >> extract_data
            extract_data >> end

    return dag
