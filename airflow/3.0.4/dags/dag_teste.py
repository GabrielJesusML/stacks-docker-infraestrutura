from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 16),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
dag = DAG(
    'test_dag',
    default_args=default_args,
    description='DAG de teste para validar funcionamento do Airflow',
    schedule=timedelta(hours=1),  # Executa a cada hora
    catchup=False,
    tags=['teste', 'exemplo'],
)

# Funções Python para as tasks
def print_hello():
    """Função simples que imprime uma mensagem"""
    print("Hello from Airflow 3.0!")
    return "Hello task completed"

def print_date(**context):
    """Função que imprime a data de execução"""
    # No Airflow 3.0, o contexto vem automaticamente quando usamos **context
    execution_date = context['data_interval_start']  # Mudança no Airflow 3.0
    print(f"Data de execução: {execution_date}")
    print(f"Data atual: {datetime.now()}")
    return execution_date.strftime("%Y-%m-%d")

def process_data(**context):
    """Função que simula processamento de dados"""
    import random
    
    # Pega o valor retornado pela task anterior usando XCom
    ti = context['ti']
    date_str = ti.xcom_pull(task_ids='print_date_task')
    
    # Simula processamento
    result = random.randint(1, 100)
    print(f"Processando dados para a data: {date_str}")
    print(f"Resultado do processamento: {result}")
    
    return {'date': date_str, 'result': result}

# Definição das tasks
# Task 1: Início (Empty Operator)
start = EmptyOperator(
    task_id='start',
    dag=dag,
)

# Task 2: Print Hello usando PythonOperator
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Task 3: Print Date
# REMOVIDO: provide_context=True (não existe mais no Airflow 3.0)
print_date_task = PythonOperator(
    task_id='print_date_task',
    python_callable=print_date,
    dag=dag,
)

# Task 4: Bash Command
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='echo "Executando comando bash" && date && echo "Airflow Version: $AIRFLOW_VERSION"',
    dag=dag,
)

# Task 5: Process Data
# REMOVIDO: provide_context=True
process_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    dag=dag,
)

# Task 6: Fim (Empty Operator)
end = EmptyOperator(
    task_id='end',
    dag=dag,
)

# Definindo as dependências (fluxo de execução)
start >> [hello_task, print_date_task]  # Start executa primeiro, depois hello e print_date em paralelo
hello_task >> bash_task  # Bash task executa após hello_task
print_date_task >> process_task  # Process task executa após print_date_task
[bash_task, process_task] >> end  # End executa após bash_task e process_task completarem