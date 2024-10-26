FROM apache/airflow:2.10.2-python3.10

ENV TZ=America/Mexico_City

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER ${AIRFLOW_UID}

ENV PYTHONPATH "${PYTHONPATH}:opt/airflow"

RUN python -m pip install --upgrade pip

COPY . .

RUN pip install -r requirements.txt