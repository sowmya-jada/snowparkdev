from snowflake.core import Root
import snowflake.connector
from snowflake.core.task import Task, StoredProcedureCall
from datetime import timedelta
from first_snowpark_project.app import procedures
from snowflake.core.task.dagv1 import DAG, DAGOperation, DAGTask, CreateMode, DAGTaskBranch
import first_snowpark_project
from snowflake.snowpark.types import StringType
from snowflake.snowpark import Session
import os


conn = snowflake.connector.connect(
    account = os.environ.get('SNOWFLAKE_ACCOUNT')
    , user = os.environ.get('SNOWFLAKE_USER')
    , password = os.environ.get('SNOWFLAKE_PASSWORD')
    , role = os.environ.get('SNOWFLAKE_ROLE')
    , warehouse = os.environ.get('SNOWFLAKE_WAREHOUSE')
    , database = os.environ.get('SNOWFLAKE_DATABASE')
    , schema = os.environ.get('SNOWFLAKE_SCHEMA')
)
print("Snowflake Connection established.")
print(conn)
root = Root(conn)
print(root)

#define your task ,y_task
my_task = Task("my_task", StoredProcedureCall(procedures.hello_procedure, stage_location="@dev_deployment"), warehouse = 'COMPUTE_WH', schedule = timedelta(hours=1))
tasks = root.databases['demo_db'].schemas['public'].tasks
tasks.create(my_task)


# create DAG TASK
with DAG(name="my_dag", schedule=timedelta(minutes=1), warehouse="compute_wh") as dag:
    #define dag tasks
    dag_task_1 = DAGTask("my_hello_task",\
             StoredProcedureCall(procedures.hello_procedure, args=["snowpark"],\
                          stage_location="@dev_deployment",\
                          packages=["snowflake-snowpark-python"],\
                          imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                          warehouse="compute_wh")
    dag_task_2 = DAGTask("my_test_task_2",\
             StoredProcedureCall(procedures.test_procedure,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    dag_task_3 = DAGTask("my_test_task_3",\
             StoredProcedureCall(procedures.test_procedure_two,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    dag_task_4 = DAGTask("my_test_task_4",\
             StoredProcedureCall(procedures.test_procedure_two,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    dag_task_1 >> dag_task_2 >>  [dag_task_3, dag_task_4]
    schema = root.databases["demo_db"].schemas["public"]
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag, CreateMode.or_replace)

def task_branch_func(session: Session) -> str:
    return "my_test_task_3"

with DAG(name="my_dag_branch", schedule=timedelta(minutes=1), warehouse="compute_wh", stage_location="@dev_deployment", use_func_return_value=True, packages=["snowflake-snowpark-python"]) as dag:
    #define dag tasks
    # dag_task_1 = DAGTask("my_hello_task",\
    #          StoredProcedureCall(procedures.hello_procedure, args=["snowpark"],\
    #                       input_types=[StringType()],\
    #                       return_type=StringType(),\
    #                       stage_location="@dev_deployment",\
    #                       packages=["snowflake-snowpark-python"],\
    #                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
    #                       warehouse="compute_wh")
    dag_task_1 = DAGTask("my_test_task",\
             StoredProcedureCall(procedures.test_procedure,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    dag_task_2 = DAGTask("my_test_task_2",\
             StoredProcedureCall(procedures.test_procedure,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    dag_task_3 = DAGTask("my_test_task_3",\
             StoredProcedureCall(procedures.test_procedure_two,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    dag_task_4 = DAGTask("my_test_task_4",\
             StoredProcedureCall(procedures.test_procedure_two,\
                                  stage_location="@dev_deployment",\
                                    packages=["snowflake-snowpark-python"],\
                                       imports=["@dev_deployment/my_snowpark_project/app.zip", "first_snowpark_project"]),\
                warehouse="compute_wh")
    task_branch = DAGTaskBranch("my_task_branch", task_branch_func, warehouse="compute_wh")
    dag_task_1 >> dag_task_2 >> task_branch
    task_branch >> [dag_task_3, dag_task_4]
    schema = root.databases["demo_db"].schemas["public"]
    dag_op = DAGOperation(schema)
    dag_op.deploy(dag, CreateMode.or_replace)
    

