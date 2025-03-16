from snowflake.core import Root
import snowflake.connector
from datetime import timedelta
from snowflake.core.task import Task, StoredProcedureCall
import procedures
import os

from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, CreateMode

#conn = snowflake.connector.connect()
#print("Connection established")
#print(conn)

print("********* snowflake account  ********")
conn = snowflake.connector.connect(
    user=os.environ.get('SNOWFLAKE_USER'),
    password=os.environ.get('SNOWFLAKE_PASSWORD'),
    account=os.environ.get('SNOWFLAKE_ACCOUNT'),
    warehouse=os.environ.get('SNOWFLAKE_WAREHOUSE'),
    database=os.environ.get('SNOWFLAKE_DATABASE'),
    schema=os.environ.get('SNOWFLAKE_SCHEMA'),
    role=os.environ.get('SNOWFLAKE_ROLE'),
)

print("Connection established")
print(conn)

root = Root(conn)
print(root)

# Create defination of task

#my_task = Task("my_task", StoredProcedureCall(procedures.hello_procedure, stage_location="@dev_deployment"), warehouse="compute_wh", schedule=timedelta(hours=1))


#tasks = root.databases["snowpark"].schemas['snowschema'].tasks
#tasks.create(my_task)

# Create dag
with DAG("dag_copy_emp", schedule=timedelta(days=1), use_func_return_value=True, stage_location='@dev_deployment', warehouse="compute_wh") as dag:
    dag_task_1 = DAGTask("copy_from_s3", StoredProcedureCall(procedures.copy_to_table_proc, packages=["snowflake-snowpark-python"], imports=["@dev_deployment/de_project_1/app.zip"], stage_location="@dev_deployment"), warehouse="compute_wh")
    dag_task_2 = DAGTask("execute_sql_statement", StoredProcedureCall(procedures.execute_sql_statement, packages=["snowflake-snowpark-python"], imports=["@dev_deployment/de_project_1/app.zip"], stage_location="@dev_deployment"), warehouse="compute_wh")
    
    dag_task_1 >> dag_task_2
    
schema = root.databases["snowpark"].schemas["snowschema"]
dag_op = DAGOperation(schema)
dag_op.deploy(dag, CreateMode.or_replace)