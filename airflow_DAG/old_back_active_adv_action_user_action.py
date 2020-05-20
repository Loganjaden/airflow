# -*- coding: utf-8 -*-

import airflow
from airflow import DAG
from airflow.hooks.presto_hook import PrestoHook
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.models.xcom import XCom
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.spark_sql_operator import SparkSqlOperator, SparkSqlHook
from airflow.models.variable import Variable
from datetime import timedelta, datetime
import json
import os
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

"""
    defined variables from databases
"""

"""
    defined global dags params
"""
default_args = {
    'owner': 'huchangyong',
    'depends_on_past': False,
    # 'start_date': airflow.utils.dates.days_ago(0,minute=30), #调度开始日期
    'start_date': datetime(2020, 5, 15, 8, 0, 0),  # 调度开始日期
    'email': ['huchangyong@zhizh.com'],  # 邮件地址
    'email_on_failure': True,  # 失败发邮件
    'email_on_retry': False,  # 重试发邮件
    'retries': 1,  # 重试次数
    'retry_delay': timedelta(minutes=1),  # 运行间隔时间

}
"""
    clean xcom 
"""


def cleanup_xcom(context, session=None):
    dag_id = context['ti']['dag_id']
    session.query(XCom).filter(XCom.dag_id == dag_id).delete()


""""

"""

af_hql_path = "/data/git_repo/bigdata_warehouse/airflow/resources/advertisement/hiveserver2/advt"
wf_data_path = "/apps/hive/warehouse/ods_user_label.db/old_back_active_adv_action_user_action"


def error_email(context):
    """
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    error_email = EmailOperator(
        task_id="error_email",
        trigger_rule=TriggerRule.ONE_FAILED,
        to=['huchangyong@zhizh.com'],
        subject="af_advt_old_back_active_adv_action_user_action error",
        html_content="af_advt_old_back_active_adv_action_user_action error",

    )
    return error_email.execute(context=context)


"""
    微信提醒
"""


def on_failure_callback(context):
    """
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    operator = SimpleHttpOperator(
        task_id='weixin_http',
        http_conn_id='http_weixin',
        method='POST',
        endpoint='',
        data=json.dumps(
            {"content": "Airflow af_advt_old_back_active_adv_action_user_action exec error!", "app": "BigdataMonitor"}),
        # data=json.dumps({"content": "Airflow exec successfull!","app":"HostMonitoring","username":"yanjiawei"}),
        headers={"Content-Type": "application/json"},
    )
    return operator.execute(context=context)


"""
    defined dag
"""
dag = DAG(
    dag_id='af_advt_old_back_active_adv_action_user_action',  # DAG名字
    default_args=default_args,
    description='一直赚打标',
    on_success_callback=cleanup_xcom,
    on_failure_callback=error_email,
    # on_failure_callback=on_failure_callback,
    schedule_interval='30 4 * * *'
)

"""
    调度任务启动
"""
start = DummyOperator(
    task_id='start',
    dag=dag)

"""
    删除目标文件
"""
delete_hdfs_file = BashOperator(
    task_id='delete_hdfs_file',
    bash_command='hadoop fs -rm -r -f ' + wf_data_path + '/_SUCCESS',
    dag=dag
)


old_back_active_adv_action_user_action_hql = HiveOperator(
    task_id='old_back_active_adv_action_user_action_hql',
    hive_cli_conn_id='hiveserver2_default',
    depends_on_past=True,
    hql=open(af_hql_path + "/hql_old_back_active_adv_action_user_action.hql", 'r').read().replace("hivevar", "hiveconf"),
    hiveconfs={'data_date': '{{ next_ds }}', 'data_date_before': '{{ yesterday_ds }}',
               'data_date_last_hour': '{{ (execution_date - macros.timedelta(hours=3)).strftime("%Y-%m-%d") }}',
               'data_date_next_hour': '{{ (execution_date + macros.timedelta(hours=3)).strftime("%Y-%m-%d") }}'},
    params={'wf_data_path': wf_data_path, 'date_enable': True},
    dag=dag
)

"""
    创建SUCCESS文件
"""
create_hdfs_file = BashOperator(
    task_id='create_hdfs_file',
    bash_command='hadoop fs -touchz ' + wf_data_path + '/_SUCCESS',
    dag=dag
)




start >> delete_hdfs_file >> old_back_active_adv_action_user_action_hql >> create_hdfs_file
