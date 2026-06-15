# 기본세팅
bash deploy.sh

# 가상환경 활성화
. locust_env/bin/activate

# docker 실행
echo "docker starting"
docker compose up -d

echo "sleep while kafka starting"
while ! nc -z localhost 19092; do
    sleep 1
done
nc -vz localhost 1909


sleep 2

:<<'END'
echo "spark consumer starting"
docker exec -d spark-marter \
        /opt/spark/bin/spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.1.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.16 \
        --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
        --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
        /opt/spark/scripts/consumer.py
END

# producer 실행
echo "locust producer starting"
locust -f ./scripts/producer.py --headless > locust.log 2>&1 &

echo "locust producer activated"

# redpanda 내 정상 적재 여부 확인
docker exec -it redpanda rpk group list
#docker logs redpanda > redpanda.log 2>&1 &

# 가상환경 및 docker 비활성화 후 종료
echo "finished"
deactivate
docker compose stop
sudo rm -rf locust-env
docker compose rm -f redpanda
docker compose down -v
