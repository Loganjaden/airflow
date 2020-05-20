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
wf_data_path = "/apps/hive/warehouse/adt_adv.db/adt_advertiser_revenue_report_ag"


def error_email(context):
    """
    Define the callback to post on Slack if a failure is detected in the Workflow
    :return: operator.execute
    """
    error_email = EmailOperator(
        task_id="error_email",
        trigger_rule=TriggerRule.ONE_FAILED,
        to=['huchangyong@zhizh.com'],
        subject="af_advt_adt_advertiser_revenue_report_ag error",
        html_content="af_advt_adt_advertiser_revenue_report_ag error",

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
            {"content": "Airflow af_adt_advertiser_revenue_report_ag exec error!", "app": "BigdataMonitor"}),
        # data=json.dumps({"content": "Airflow exec successfull!","app":"HostMonitoring","username":"yanjiawei"}),
        headers={"Content-Type": "application/json"},
    )
    return operator.execute(context=context)


"""
    defined dag
"""
dag = DAG(
    dag_id='af_advt_adt_advertiser_revenue_report_ag',  # DAG名字
    default_args=default_args,
    description='CPI上游收益波动',
    on_success_callback=cleanup_xcom,
    on_failure_callback=error_email,
    # on_failure_callback=on_failure_callback,
    schedule_interval='30 4,8,12,16,18,20 * * *'
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
    bash_command='hadoop fs -rm -r -f ' + wf_data_path + '/dt={{ yesterday_ds }}/_SUCCESS',
    dag=dag
)

delete_hdfs_file_2 = BashOperator(
    task_id='delete_hdfs_file_2',
    bash_command='hadoop fs -rm -r -f ' + wf_data_path + '/dt={{ ds }}/_SUCCESS',
    dag=dag
)

af_adt_advertiser_revenue_report_ag_hql = HiveOperator(
    task_id='af_adt_advertiser_revenue_report_ag_hql',
    hive_cli_conn_id='hiveserver2_default',
    depends_on_past=True,
    hql=open(af_hql_path + "/hql_adt_advertiser_revenue_report_ag.hql", 'r').read().replace("hivevar", "hiveconf"),
    hiveconfs={'data_date': '{{ ds }}', 'data_date_before': '{{ yesterday_ds }}',
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
    bash_command='hadoop fs -touchz ' + wf_data_path + '/dt={{ yesterday_ds }}/_SUCCESS',
    dag=dag
)

"""
    创建SUCCESS文件
"""
create_hdfs_file_2 = BashOperator(
    task_id='create_hdfs_file_2',
    bash_command='hadoop fs -touchz ' + wf_data_path + '/dt={{ ds }}/_SUCCESS',
    dag=dag
)

"""
    kylin 调用
"""
af_project_name = 'advt_revenue'
cube_name = 'cube_advt_revenue_adt_advertiser_revenue_report_ag'
af_kylin_url = 'http://zzdz0420180014.hk.batmobi.cn:7070'
kylin_script_path = '/data/git_repo/bigdata_warehouse/oozie_project/workspaces/resources/common/script/kylin_rebuild_cube.sh'
run_kylin = BashOperator(
    task_id='kylin_cube_' + cube_name + '_1',
    bash_command='source /etc/profile; '
                 'sh ' + kylin_script_path + ' ' + af_kylin_url + ' ' + af_project_name + ' ' + cube_name + '  {{ ds }}',
    params={'table_name': cube_name},
    dag=dag
)

run_kylin_2 = BashOperator(
    task_id='kylin_cube_' + cube_name + '_2',
    bash_command='source /etc/profile; '
                 'sh ' + kylin_script_path + ' ' + af_kylin_url + ' ' + af_project_name + ' ' + cube_name + '{{ yesterday_ds }}',
    params={'table_name': cube_name},
    dag=dag
)

start >> [delete_hdfs_file, delete_hdfs_file_2] >> af_adt_advertiser_revenue_report_ag_hql >> [create_hdfs_file,create_hdfs_file_2]
create_hdfs_file >> run_kylin_2
create_hdfs_file_2 >> run_kylin