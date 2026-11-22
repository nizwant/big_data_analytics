ARG AIRFLOW_VERSION=3.4.0
ARG PYTHON_VERSION=3.12

# The slim image has Airflow without the ~60 providers of the default image
# (about 0.8 GB instead of 2.4 GB); requirements.txt adds the few that are needed.
FROM apache/airflow:slim-${AIRFLOW_VERSION}-python${PYTHON_VERSION}
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION

# Versions come from the constraints file of this Airflow release, i.e. the set it was
# tested with; apache-airflow is listed so pip cannot up- or downgrade it.
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
