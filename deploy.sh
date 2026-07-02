echo "Infra auto setup started"

echo "python & locust installation"
sudo apt update
sudo apt install -y python3 python3-venv netcat-openbsd

python3 -m venv locust_env

. locust_env/bin/activate

pip install locust pyyaml confluent_kafka

deactivate

echo "docker installation - debian"
sudo apt install -y gnome-terminal
sudo apt-get install -y ./docker-desktop-amd64.deb

# spark 요구 패키지 저장용 캐시 폴더 생성
mkdir -p ./spark_cache
chmod 777 ./spark_cache
touch ./scripts/dummy_init.py

echo "spark package download"
wget -q -O ./spark_cache/gcs-connector-hadoop3-2.2.16-shaded.jar https://github.com/GoogleCloudDataproc/hadoop-connectors/releases/download/v2.2.16/gcs-connector-hadoop3-2.2.16-shaded.jar
# 더미 파일 활용 -> 패키지 다운로드
docker compose up -d spark-master
docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0 \
        --jars /tmp/.ivy2/gcs-connector-hadoop3-2.2.16-shaded.jar \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        /opt/spark/scripts/dummy_init.py
docker compose down


:<<'END'

mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env

echo "Airflow init started"
docker compose up airflow-init

END

echo "Infra setup & auto-start completed"
