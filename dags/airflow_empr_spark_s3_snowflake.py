import boto3
import logging
import airflow
from airflow import DAG
try:
  # Airflow 2+ preferred imports
  from airflow.operators.python import PythonOperator
  from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
  from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
  from airflow.operators.bash import BashOperator
except Exception:
  # Fallback to older locations if providers aren't available in the environment
  from airflow.operators.python_operator import PythonOperator
  try:
    from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
    from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
  except Exception:
    SnowflakeOperator = None
    SnowflakeHook = None
  from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from time import sleep
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="snowflake_automation_dag", default_args=args, schedule_interval=None
)

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# Allow overriding EMR roles via environment variables. Provide sensible defaults commonly used by AWS.
EMR_JOBFLOW_ROLE = os.getenv("EMR_JOBFLOW_ROLE", "EMR_DefaultRole")
EMR_EC2_ROLE = os.getenv("EMR_EC2_ROLE", "EMR_EC2_DefaultRole")
EMR_EC2_KEY_NAME = os.getenv("EMR_EC2_KEY_NAME", "helloairflow")
# Instance types (configurable) - m5.large is a cost-conscious, modern, widely available choice
EMR_MASTER_INSTANCE_TYPE = os.getenv("EMR_MASTER_INSTANCE_TYPE", "m5.large")
EMR_CORE_INSTANCE_TYPE = os.getenv("EMR_CORE_INSTANCE_TYPE", "m5.large")

client = boto3.client('emr', region_name='us-east-1',aws_access_key_id=AWS_ACCESS_KEY_ID,aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

def create_emr_cluster():
  # Preflight: validate that the referenced IAM instance profile / role exists to avoid ValidationException
  iam = boto3.client('iam', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
  try:
    iam.get_role(RoleName=EMR_EC2_ROLE)
  except Exception:
    # The common cause is that the EMR default roles haven't been created in this account/region
    raise RuntimeError(
      f"EMR EC2 role '{EMR_EC2_ROLE}' not found. Create default EMR roles with: `aws emr create-default-roles` "
      "or set EMR_EC2_ROLE/EMR_JOBFLOW_ROLE env vars to valid roles in your account."
    )

  # Validate EC2 key pair exists to avoid EMR terminating with invalid key name
  ec2 = boto3.client('ec2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name='us-east-1')
  try:
    ec2.describe_key_pairs(KeyNames=[EMR_EC2_KEY_NAME])
  except Exception:
    raise RuntimeError(
      f"EC2 key pair '{EMR_EC2_KEY_NAME}' not found in region us-east-1. Create or provide a valid key pair via EMR_EC2_KEY_NAME env var."
    )

  cluster_resp = client.run_job_flow(
    Name="transient_demo_testing",
    Instances={
    'InstanceGroups': [
  {
  'Name': "Master",
  'Market': 'ON_DEMAND',
  'InstanceRole': 'MASTER',
  'InstanceType': EMR_MASTER_INSTANCE_TYPE,
  'InstanceCount': 1,
  },
  {
  'Name': "Slave",
  'Market': 'ON_DEMAND',
  'InstanceRole': 'CORE',
  'InstanceType': EMR_CORE_INSTANCE_TYPE,
  'InstanceCount': 2,
  }
    ],
  'Ec2KeyName': EMR_EC2_KEY_NAME,
    'KeepJobFlowAliveWhenNoSteps': True,
    'TerminationProtected': False,
    'Ec2SubnetId': 'subnet-060ff617833f9237d',
    },
    LogUri="s3://hv-emr-logs/",
    ReleaseLabel= 'emr-5.33.0',
    BootstrapActions=[],
    VisibleToAllUsers=True,
    JobFlowRole=EMR_EC2_ROLE,
    ServiceRole=EMR_JOBFLOW_ROLE,
    Applications = [ {'Name': 'Spark'},{'Name':'Hive'}])
  # run_job_flow returns a dict containing 'JobFlowId' — return the id string for downstream calls
  jobflow_id = cluster_resp.get('JobFlowId')
  print("The cluster started with JobFlowId : {}".format(jobflow_id))
  return jobflow_id


  
def add_step_emr(cluster_id,jar_file,step_args):
  # Normalize cluster_id: XCom content may be a dict (old code) or a string JobFlowId (newer return)
  if hasattr(cluster_id, 'get'):
    # dict-like
    cluster_id = cluster_id.get('JobFlowId') or cluster_id.get('jobFlowId') or cluster_id.get('jobflowid')

  print("The cluster id : {}".format(cluster_id))
  print("The step to be added : {}".format(step_args))
  # Wait for cluster readiness (or detect terminal state) before attempting to add steps
  state, reason = wait_for_cluster_ready(cluster_id)
  allowed_states = {"STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"}
  if state not in allowed_states:
    raise RuntimeError(
      f"Cannot add step to cluster {cluster_id} because it is in state '{state}'. "
      f"StateChangeReason={reason}. A job flow that is shutting down, terminated, or finished may not be modified."
    )
  response = client.add_job_flow_steps(
  JobFlowId=cluster_id,
  Steps=[
  {
    'Name': 'test12',
    'ActionOnFailure':'CONTINUE',
    'HadoopJarStep': {
  'Jar': jar_file,
  'Args': step_args
  }
  },
  ]
  )
  print("The emr step is added")
  return response['StepIds'][0]
  
def get_status_of_step(cluster_id,step_id):
  response = client.describe_step(
    ClusterId=cluster_id,
    StepId=step_id
  )
  return response['Step']['Status']['State']


def wait_for_cluster_ready(cluster_id, timeout_seconds=120, poll_interval=5):
  """Poll describe_cluster until cluster state is RUNNING or WAITING, or raise after timeout.
  Returns the final state and the optional StateChangeReason dict.

  Defaults are reduced so callers fail fast during development: 2 minute timeout, poll every 5s.
  """
  import time
  # Allow overriding defaults from environment for longer waits in CI/production
  try:
    env_timeout = os.getenv("EMR_WAIT_TIMEOUT_SECONDS")
    if env_timeout:
      timeout_seconds = int(env_timeout)
  except Exception:
    logger.warning("Invalid EMR_WAIT_TIMEOUT_SECONDS value, using %s", timeout_seconds)
  try:
    env_poll = os.getenv("EMR_POLL_INTERVAL_SECONDS")
    if env_poll:
      poll_interval = int(env_poll)
  except Exception:
    logger.warning("Invalid EMR_POLL_INTERVAL_SECONDS value, using %s", poll_interval)

  start = time.time()
  logger.info("Waiting up to %s seconds for cluster %s to become RUNNING/WAITING (poll interval %s s)", timeout_seconds, cluster_id, poll_interval)
  while True:
    try:
      info = client.describe_cluster(ClusterId=cluster_id)
      cluster = info.get('Cluster', {})
      status = cluster.get('Status', {})
      state = status.get('State')
      reason = status.get('StateChangeReason')
    except Exception as e:
      raise RuntimeError(f"Failed to describe cluster {cluster_id}: {e}")

    logger.info("Cluster %s state=%s", cluster_id, state)

    # If AWS returns PROVISIONING messages (EC2 capacity), log a hint for remediation.
    if reason and isinstance(reason, dict) and 'Message' in reason:
      msg = reason.get('Message')
      if 'Provisioning Amazon EC2 capacity' in msg:
        logger.warning(
          "Cluster %s is provisioning EC2 capacity. This can be transient; consider increasing EMR_WAIT_TIMEOUT_SECONDS, switching instance type, AZ, or using On-Demand instances.",
          cluster_id,
        )

    if state in ("RUNNING", "WAITING"):
      return state, reason

    if time.time() - start > timeout_seconds:
      raise RuntimeError(
        f"Timed out waiting for cluster {cluster_id} to become RUNNING/WAITING; last state={state}, reason={reason}"
      )

    # If the cluster is in a terminal state, return immediately with that state
    if state in ("TERMINATING", "TERMINATED", "TERMINATED_WITH_ERRORS"):
      return state, reason

    time.sleep(poll_interval)
  
  
def wait_for_step_to_complete(cluster_id,step_id):
  print("The cluster id : {}".format(cluster_id))
  print("The emr step id : {}".format(step_id))
  while True:
    try:
      status=get_status_of_step(cluster_id,step_id)
      if status =='COMPLETED':
        break
      else:
        print("The step is {}".format(status))
        sleep(40)

    except Exception as e:
      logging.info(e)
	  

def terminate_cluster(cluster_id):
    try:
        client.terminate_job_flows(JobFlowIds=[cluster_id])
        logger.info("Terminated cluster %s.", cluster_id)
    except ClientError:
        logger.exception("Couldn't terminate cluster %s.", cluster_id)
        raise
		
with dag:
  create_emr_cluster = PythonOperator(
  task_id='create_emr_cluster',
  python_callable=create_emr_cluster,
  dag=dag, 
  )
  ingest_layer = PythonOperator(
  task_id='ingest_layer',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull(task_ids="create_emr_cluster") }}','s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar',[ 's3://hv-irisdata/scripts/ingest.sh']],
  dag=dag, 
  )
  poll_step_layer = PythonOperator(
  task_id='poll_step_layer',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull(task_ids="create_emr_cluster") }}','{{ ti.xcom_pull(task_ids="ingest_layer") }}'],
  dag=dag, 
  )
  transform_layer = PythonOperator(
  task_id='transform_layer',
  python_callable=add_step_emr,
  op_args=['{{ ti.xcom_pull(task_ids="create_emr_cluster") }}','command-runner.jar',[ 'spark-submit',
            '--master', 'yarn',
            '--deploy-mode', 'cluster',
            's3://hv-irisdata/scripts/transform.py']],
  dag=dag, 
  )
  poll_step_layer2 = PythonOperator(
  task_id='poll_step_layer2',
  python_callable=wait_for_step_to_complete,
  op_args=['{{ ti.xcom_pull(task_ids="create_emr_cluster") }}','{{ ti.xcom_pull(task_ids="transform_layer") }}'],
  dag=dag, 
  )
  terminate_emr_cluster = PythonOperator(
  task_id='terminate_emr_cluster',
  python_callable=terminate_cluster,
  op_args=['{{ ti.xcom_pull(task_ids="create_emr_cluster") }}'],
  dag=dag, 
  )
  # If SnowflakeOperator (provider) isn't available in this environment, create a lightweight fallback
  def _snowflake_missing():
    logger.warning("SnowflakeOperator not available: skipping Snowflake refresh step.")

  if SnowflakeOperator is not None:
    snowflake_load = SnowflakeOperator(
      task_id="snowflake_load",
      sql="""ALTER EXTERNAL TABLE ASTRO_SDK_DB.PUBLIC.Iris_dataset REFRESH""",
      snowflake_conn_id="snowflake_default2",
    )
  else:
    # create a simple PythonOperator fallback that logs a warning at runtime
    snowflake_load = PythonOperator(
      task_id="snowflake_load",
      python_callable=_snowflake_missing,
    )

create_emr_cluster >> ingest_layer >> poll_step_layer >> transform_layer >> poll_step_layer2 >> terminate_emr_cluster >> snowflake_load