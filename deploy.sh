echo "Infra auto setup started"

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

echo "Airflow init started"
docker compose up airflow-init

echo "docker compose up & setup waiting"
docker compose up -d

echo "spark consumer starting"
docker exec -d spark-marter \
	/opt/spark/bin/spark-submit \
	--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.16 \
	--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
	--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
	/opt/spark/scripts/consumer.py

echo "locust producer starting"
locust -f producer.py --headless

echo "Infra setup & auto-start completed"
