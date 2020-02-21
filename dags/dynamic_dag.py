import json
import logging
import os

import yaml
from airflow import DAG
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
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

    # Docker repository info
    DOCKER_REPOSITORY_CONFIG = Variable.get('DOCKER_REPOSITORY', deserialize_json=True)
    image_url = DOCKER_REPOSITORY_CONFIG['image_url']
    docker_url = DOCKER_REPOSITORY_CONFIG['docker_url']

    # Config file
    configFile = args['configFile']
    logging.info(configFile)

    # DB connection
    DB_CONFIG = Variable.get(configFile['db_connection'], deserialize_json=True)

    # ETL parameters
    loader_image = args['image']

    host = DB_CONFIG['host']
    user = DB_CONFIG['user']
    port = DB_CONFIG['port']
    password = DB_CONFIG['password']

    db_name = configFile['db_name']
    bucket_name = configFile['bucket_name']
    prefix = configFile['prefix']

    tables = configFile['tables']
    logging.info('Entrying into orchestration step...')

    for table in tables:
        for table, config_dict in table.items():
            logging.info('Orchestrating etl for table: {}'.format(table))

            query = "select * from {}.{} where {} like '%{}%'"
            logging.info('Query: {}'.format(query))
            extract_data = DockerOperator(
                task_id='{}-data-extraction'.format(table),
                image='{}/{}'.format(image_url, loader_image),
                docker_url=docker_url,
                environment={
                    'exec_date': "{{ ds }}",
                    'date_to_work': "{{ prev_ds }}",
                    'dbhost': host, 'dbuser': user, 'dbport': port, 'dbpassword': password,
                    'dbname': db_name, 'bucket_name': bucket_name,
                    'prefix': prefix, 'table': table, 'columnTypes': config_dict['columnTypes'],
                    'query': query.format(db_name, table, config_dict['filteredBy'], "{{ prev_ds }}"),
                    'AWS_ACCESS_KEY_ID': AWS_KEYS['AWS_ACCESS_KEY_ID'],
                    'AWS_SECRET_ACCESS_KEY': AWS_KEYS['AWS_SECRET_ACCESS_KEY']
                }, dag=dag)

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
