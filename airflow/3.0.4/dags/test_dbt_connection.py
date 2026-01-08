"""
Script para testar a configuração do DBT com o Airflow
Execute este script no Airflow para validar a configuração
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook  # Corrigido: import correto para Airflow 3

def test_connection(**context):
    """Testa a conexão db_isabella_mori"""
    try:
        # Testar a conexão
        hook = PostgresHook(postgres_conn_id='db_isabella_mori')
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Testar query simples
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        print(f"✅ Conexão bem-sucedida! Resultado do teste: {result}")
        
        # Verificar schemas
        cursor.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'bronze', 'silver', 'gold', 'analytics')
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()
        print(f"📁 Schemas encontrados: {[s[0] for s in schemas]}")
        
        # Se os schemas não existirem, criar
        for schema in ['raw', 'bronze', 'silver', 'gold']:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            print(f"✅ Schema {schema} garantido")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return "Teste concluído com sucesso!"
        
    except Exception as e:
        print(f"❌ Erro ao testar conexão: {str(e)}")
        raise

def test_dbt_installation(**context):
    """Testa se o DBT está instalado corretamente"""
    import subprocess
    
    try:
        # Testar DBT
        result = subprocess.run(
            ['/opt/airflow/dbt_venv/bin/dbt', '--version'],
            capture_output=True,
            text=True
        )
        print(f"✅ DBT instalado:\n{result.stdout}")
        
        # Verificar se o projeto existe
        import os
        project_path = '/opt/airflow/dags/dbt/dw_im'
        if os.path.exists(project_path):
            print(f"✅ Projeto DBT encontrado em: {project_path}")
            
            # Listar arquivos principais
            for file in ['dbt_project.yml', 'profiles.yml']:
                file_path = os.path.join(project_path, file)
                if os.path.exists(file_path):
                    print(f"  ✅ {file} encontrado")
                else:
                    print(f"  ⚠️ {file} não encontrado (pode usar connection do Airflow)")
            
            # Verificar pastas de modelos
            models_path = os.path.join(project_path, 'models')
            if os.path.exists(models_path):
                subdirs = [d for d in os.listdir(models_path) if os.path.isdir(os.path.join(models_path, d))]
                print(f"  📁 Pastas em models/: {subdirs}")
        else:
            print(f"❌ Projeto DBT não encontrado em: {project_path}")
            print("  Por favor, copie seu projeto DBT para esta pasta")
            
    except Exception as e:
        print(f"❌ Erro ao testar DBT: {str(e)}")
        raise

# DAG de teste
with DAG(
    dag_id='test_dbt_setup',
    description='Testa a configuração do DBT no Airflow',
    schedule=None,  # Manual - Corrigido para Airflow 3
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'dbt', 'setup'],
) as dag:
    
    test_db = PythonOperator(
        task_id='test_database_connection',
        python_callable=test_connection,
    )
    
    test_dbt = PythonOperator(
        task_id='test_dbt_installation',
        python_callable=test_dbt_installation,
    )
    
    test_db >> test_dbt