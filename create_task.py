from snowflake.core import Root
import snowflake.connector
from snowflake.core.task import Task, StoredProcedureCall
from datetime import timedelta
from first_snowpark_project.app import procedures
from snowflake.core.task.dagv1 import DAG, DAGOperation, DAGTask, CreateMode

conn = snowflake.connector.connect()
print("Snowflake Connection established.")
print(conn)
root = Root(conn)
print(root)

#define your task ,y_task
my_task = Task("my_task", StoredProcedureCall(procedures.hello_procedure, stage_location="@dev_deployment"), warehouse = 'COMPUTE_WH', schedule = timedelta(hours=1))
tasks = root.databases['demo_db'].schemas['public'].tasks
# tasks.create(my_task)

# create DAG TASK
with DAG(name="my_dag", schedule=timedelta(minutes=1)) as dag:
    #define dag tasks
    dag_task_1 = DAGTask("my_hello_task",\
             StoredProcedureCall(procedures.hello_procedure, args=["snowpark"], stage_location="@dev_deployment"),\
                warehouse="compute_wh")
    dag_task_2 = DAGTask("my_test_task",\
             StoredProcedureCall(procedures.test_procedure, stage_location="@dev_deployment"),\
                warehouse="compute_wh")
    dag_task_1 >> dag_task_2
    schema = root.databases["demo_db"].schemas["public"]
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag, CreateMode.or_replace)
    

