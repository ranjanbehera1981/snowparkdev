from snowflake.core import Root
import snowflake.connector
from datetime import timedelta
from snowflake.core.task import Task, StoredProcedureCall
import procedures
import os

from snowflake.core.task.dagv1 import DAG, DAGTask, DAGOperation, CreateMode

conn = snowflake.connector.connect()
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
    dag_task_1 = DAGTask("copy_from_s3", StoredProcedureCall(procedures.copy_to_table_proc, packages=["snowflake-snowpark-python"], imports=["@dev_deployment/my_de_project_1/app.zip"], stage_location="@dev_deployment"), warehouse="compute_wh")

schema = root.databases["snowpark"].schemas["snowschema"]
dag_op = DAGOperation(schema)
dag_op.deploy(dag, CreateMode.or_replace)