FROM apache/airflow:3.0.4-python3.12

# Mudar para root para instalar dependências do sistema
USER root

# Instalar dependências do sistema necessárias
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    git \
    python3-dev \
    libpq-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Criar ambiente virtual para DBT e instalar dbt-postgres
RUN python -m venv /opt/airflow/dbt_venv && \
    source /opt/airflow/dbt_venv/bin/activate && \
    pip install --no-cache-dir \
    dbt-postgres \
    && deactivate

# Ajustar permissões do ambiente virtual para o usuário airflow
RUN chown -R airflow:root /opt/airflow/dbt_venv && \
    chmod -R 755 /opt/airflow/dbt_venv

# Voltar para o usuário airflow
USER airflow

# Copiar requirements.txt
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# Instalar o Airflow com extras do Celery
RUN pip install --no-cache-dir \
    'apache-airflow[celery,redis,postgres]==3.0.4' \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.4/constraints-3.12.txt"

# Instalar o FAB provider e outras dependências
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.4/constraints-3.12.txt"

# Limpar cache do pip
RUN pip cache purge

# Variável de ambiente para o DBT
ENV DBT_EXECUTABLE_PATH=/opt/airflow/dbt_venv/bin/dbt

# Verificar instalações
RUN python -c "import airflow; print(f'Airflow version: {airflow.__version__}')" && \
    python -c "from airflow.providers.fab import __version__; print(f'FAB Provider version: {__version__}')" && \
    python -c "import flask_session; print('Flask-Session installed')" && \
    python -c "import airflow_code_editor; print('Airflow Code Editor installed')" && \
    git --version && \
    /opt/airflow/dbt_venv/bin/dbt --version