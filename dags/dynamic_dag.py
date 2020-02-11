import json
import logging
import os
import shutil

import yaml
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from git import Repo


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


YAML_CONFIG_FOLDER = '/Users/leonardodealmeidasantos/workspace/python/dags-config'
CONFIG_REPO = 'https://github.com/leoalmeidasant/dags-config.git'

# if os.path.exists(YAML_CONFIG_FOLDER):
#     shutil.rmtree(YAML_CONFIG_FOLDER)
# Repo.clone_from(url=CONFIG_REPO, to_path=YAML_CONFIG_FOLDER)
# logging.info("Cloned with successful")

for filename in os.listdir(YAML_CONFIG_FOLDER):
    if filename.endswith('.yaml'):
        with open('{path}/{file}'.format(path=YAML_CONFIG_FOLDER, file=filename)) as f:
            # use safe_load instead load
            config_file = yaml.safe_load(f)
            logging.info(config_file)

            # Dag info
            dag_id = config_file['job']
            default_args = json.loads(config_file['default_args'])
            schedule = config_file['schedule']
            catchup = False

            # Defining DAG args
            # loader_image = loader_images.get(config_file['type'])
            args = {'config_file': config_file}

            # The appropiate factory method is invoked according to the database engine type
            if config_file['type'] == 'mysql':
                globals()[dag_id] = mysql_to_s3_loader(dag_id, schedule, default_args, catchup, args)
