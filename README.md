# Proyecto Spark, Prometheus, Grafana, Alertmanager y Ganglia

Proyecto de despliegue y monitorización de un clúster Spark con Docker, Prometheus, Grafana, Alertmanager y Ganglia.

## Estructura del proyecto

- `monitoring/`: configuración de monitorización y alertas
  - `docker-compose.monitoring.yml`
  - `docker-compose.ganglia.yml`
  - `prometheus.yml`
  - `alertmanager.yml`
  - `rules.yml`
  - `grafana.ini`
  - `dockerfile`

- `spark/`: configuración del clúster Spark y job de análisis
  - `docker-compose.spark.yml`
  - `metrics.properties`
  - `job.py`
  - `netflix_titles.csv`

## Tecnologías utilizadas

- Docker
- Apache Spark
- Prometheus
- Grafana
- Alertmanager
- Ganglia

## Objetivo

Desplegar un clúster Spark con 1 nodo master y 3 workers, monitorizarlo con Prometheus y Grafana, configurar alertas y validar la integración con Ganglia.

## Autor

Eduardo Viera
