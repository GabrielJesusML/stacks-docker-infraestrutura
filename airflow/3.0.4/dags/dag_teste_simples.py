"""
DAG de Teste Completa para Airflow 3.0 com CeleryExecutor
Versão otimizada usando TaskFlow API e melhores práticas do Airflow 3.0
"""

from datetime import datetime, timedelta
import random
import time
import logging
from typing import Any, Dict, List

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import chain  # Para dependências mais limpas

# Configuração de logging
logger = logging.getLogger(__name__)

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow_test',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='test_complete_airflow_3_optimized',
    default_args=default_args,
    description='Optimized test DAG for Airflow 3.0 with TaskFlow API',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['test', 'validation', 'celery', 'airflow3', 'taskflow'],
)
def test_airflow_3_dag():
    """
    DAG principal usando TaskFlow API do Airflow 3.0
    """
    
    @task
    def check_environment() -> Dict[str, Any]:
        """Verifica o ambiente e informações básicas do Airflow"""
        import os
        import platform
        from airflow.operators.python import get_current_context
        
        # Obtém contexto usando a nova API
        context = get_current_context()
        
        env_info = {
            'hostname': platform.node(),
            'python_version': platform.python_version(),
            'airflow_home': os.environ.get('AIRFLOW_HOME', 'Not Set'),
            'executor': os.environ.get('AIRFLOW__CORE__EXECUTOR', 'Unknown'),
            'dag_run_id': context['dag_run'].run_id,
            'data_interval_start': str(context['data_interval_start']),
            'data_interval_end': str(context['data_interval_end']),  # Novo no 3.0
            'task_instance': context['ti'].task_id,
            'worker_pod': os.environ.get('HOSTNAME', 'Unknown')
        }
        
        logger.info("=== Environment Information ===")
        for key, value in env_info.items():
            logger.info(f"{key}: {value}")
        
        return env_info
    
    @task
    def generate_test_data() -> Dict[str, Any]:
        """Gera dados de teste"""
        from datetime import datetime
        
        logger.info("Generating test data...")
        
        test_data = {
            'timestamp': datetime.now().isoformat(),
            'records_count': random.randint(100, 1000),
            'batch_id': f"batch_{random.randint(1000, 9999)}",
            'data_samples': [
                {'id': i, 'value': random.random() * 100}
                for i in range(5)
            ]
        }
        
        logger.info(f"Generated {test_data['records_count']} records")
        logger.info(f"Batch ID: {test_data['batch_id']}")
        
        return test_data
    
    @task
    def process_data_worker(task_number: int, test_data: Dict[str, Any]) -> Dict[str, Any]:
        """Processa dados - usando TaskFlow API para passar dados automaticamente"""
        import os
        
        if not test_data:
            raise ValueError("No data received from generate_data task")
        
        worker_info = {
            'task_number': task_number,
            'hostname': os.environ.get('HOSTNAME', 'Unknown'),
            'worker_name': f"worker_{task_number}",
            'processed_records': test_data['records_count'] // 3,
            'batch_id': test_data['batch_id']
        }
        
        logger.info(f"=== Processing on Worker {task_number} ===")
        logger.info(f"Hostname: {worker_info['hostname']}")
        logger.info(f"Processing {worker_info['processed_records']} records")
        
        processing_time = random.uniform(2, 5)
        time.sleep(processing_time)
        
        worker_info['processing_result'] = {
            'status': 'success',
            'processing_time': processing_time,
            'records_processed': worker_info['processed_records'],
            'errors': 0
        }
        
        logger.info(f"Worker {task_number} completed successfully!")
        return worker_info
    
    @task
    def aggregate_results(worker_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Agrega resultados de todos os workers"""
        logger.info(f"=== Aggregating Results from {len(worker_results)} Workers ===")
        
        total_records = sum(w['processing_result']['records_processed'] for w in worker_results)
        total_time = sum(w['processing_result']['processing_time'] for w in worker_results)
        
        aggregated = {
            'total_workers': len(worker_results),
            'total_records_processed': total_records,
            'total_processing_time': total_time,
            'average_time_per_worker': total_time / len(worker_results) if worker_results else 0,
            'workers_info': [
                {
                    'worker': w['worker_name'],
                    'hostname': w['hostname'],
                    'records': w['processing_result']['records_processed']
                }
                for w in worker_results
            ]
        }
        
        logger.info(f"Total records processed: {aggregated['total_records_processed']}")
        logger.info(f"Total processing time: {aggregated['total_processing_time']:.2f}s")
        
        return aggregated
    
    @task
    def test_database_connection() -> Dict[str, Any]:
        """Testa conexão com banco de dados"""
        import os
        
        logger.info("=== Testing Database Connection ===")
        
        connection_params = {
            'host': 'postgres',
            'port': 5432,
            'database': os.environ.get('POSTGRES_DB', 'airflow'),
            'user': os.environ.get('SERVICE_USER_POSTGRES', 'airflow'),
            'password': os.environ.get('SERVICE_PASSWORD_POSTGRES', 'airflow')
        }
        
        try:
            import psycopg2
            
            logger.info(f"Connecting to PostgreSQL at {connection_params['host']}:{connection_params['port']}")
            
            conn = psycopg2.connect(**connection_params)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    current_database() as database,
                    current_user as user,
                    version() as postgres_version,
                    now() as current_time
            """)
            
            result = cursor.fetchone()
            
            db_info = {
                'status': 'connected',
                'database': result[0],
                'user': result[1],
                'postgres_version': result[2][:50],
                'current_time': str(result[3])
            }
            
            logger.info(f"✅ Database connection successful!")
            logger.info(f"Database: {db_info['database']}")
            
            cursor.close()
            conn.close()
            
            return db_info
            
        except Exception as e:
            logger.warning(f"⚠️ Could not connect to database: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'note': 'This is optional - DAG will continue'
            }
    
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def test_failure_with_retry() -> str:
        """Testa mecanismo de retry"""
        from airflow.operators.python import get_current_context
        
        context = get_current_context()
        ti = context['ti']
        try_number = ti.try_number
        
        logger.info(f"=== Testing Retry Mechanism ===")
        logger.info(f"Current attempt: {try_number}")
        
        if try_number == 1:
            logger.warning("Simulating failure on first attempt...")
            time.sleep(2)
            raise AirflowException("Simulated failure for testing retry mechanism")
        
        logger.info("Success on retry attempt!")
        return f"Task succeeded on attempt {try_number}"
    
    @task(trigger_rule='all_done')
    def final_validation(
        env_info: Dict[str, Any],
        test_data: Dict[str, Any],
        aggregated: Dict[str, Any],
        db_info: Dict[str, Any],
        retry_result: str
    ) -> None:
        """Validação final e resumo da execução"""
        logger.info("=" * 50)
        logger.info("=== FINAL VALIDATION REPORT ===")
        logger.info("=" * 50)
        
        validations = {
            'Environment Check': '✅' if env_info else '❌',
            'Data Generation': '✅' if test_data else '❌',
            'Worker Distribution': '✅' if aggregated and aggregated['total_workers'] == 3 else '❌',
            'Database Connection': '✅' if db_info and db_info.get('status') == 'connected' else '⚠️ Optional',
            'Retry Mechanism': '✅' if retry_result else '❌',
            'XCom Communication': '✅' if all([env_info, test_data, aggregated]) else '❌',
        }
        
        for check, status in validations.items():
            logger.info(f"{check}: {status}")
        
        if aggregated:
            logger.info("\n=== Worker Distribution Summary ===")
            for worker in aggregated['workers_info']:
                logger.info(f"- {worker['worker']} on {worker['hostname']}: {worker['records']} records")
        
        essential_tests = {k: v for k, v in validations.items() if k != 'Database Connection'}
        all_passed = all(status == '✅' for status in essential_tests.values())
        
        if all_passed:
            logger.info("\n🎉 ALL ESSENTIAL TESTS PASSED SUCCESSFULLY! 🎉")
            logger.info("Your Airflow 3.0 installation with CeleryExecutor is working correctly!")
        else:
            failed_tests = [check for check, status in essential_tests.items() if status == '❌']
            logger.warning(f"\n⚠️ Some tests failed: {', '.join(failed_tests)}")
    
    # Bash operator mantido como está (não tem versão TaskFlow)
    check_system = BashOperator(
        task_id='check_system',
        bash_command="""
        echo "=== System Check ==="
        echo "Hostname: $(hostname)"
        echo "Current directory: $(pwd)"
        echo "User: $(whoami)"
        echo "Python: $(python --version)"
        echo "Airflow Home: $AIRFLOW_HOME"
        echo "Executor: ${AIRFLOW__CORE__EXECUTOR:-Not Set}"
        echo "===================="
        """
    )
    
    # Execução das tasks usando TaskFlow API
    env_info = check_environment()
    test_data = generate_test_data()
    
    # Processamento paralelo
    worker_results = []
    for i in range(1, 4):
        worker_result = process_data_worker.override(task_id=f'process_data_{i}')(i, test_data)
        worker_results.append(worker_result)
    
    aggregated = aggregate_results(worker_results)
    db_info = test_database_connection()
    retry_result = test_failure_with_retry()
    
    # Validação final
    final = final_validation(env_info, test_data, aggregated, db_info, retry_result)
    
    # Definir dependências adicionais
    [env_info, check_system] >> test_data
    test_data >> worker_results[0]

# Instanciar a DAG
dag_instance = test_airflow_3_dag()