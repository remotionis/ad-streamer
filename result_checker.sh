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

# 기존 토픽 청소
docker exec -t redpanda rpk topic delete ad-stream-topic 2>/dev/null
docker exec -t redpanda rpk topic create ad-stream-topic -p 10

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
echo "locust starting"
# locust -f ./scripts/producer.py --headless > locust.log 2>&1 &
locust -f ./scripts/producer.py --master --master-bind-host=127.0.0.1 > locust_master.log 2>&1 &
MASTER_PID=$!
echo "locust master activated - PID: $MASTER_PID"
sleep 2

WORKER_COUNT=4
for i in $(seq 1 $WORKER_COUNT)
do
        locust -f ./scripts/producer.py --worker --master-host=127.0.0.1 > locust_worker_$i.log 2>&1 &
        echo "locust worker $i activated"
done

# 모니터링을 위한 루프
MAX_TIMEOUT=120 
START_TIME=$SECONDS

while kill -0 $MASTER_PID 2>/dev/null; do
        # 타임아웃 여부
        ELAPSED=$(( SECONDS - START_TIME ))
        if [ $ELAPSED -ge $MAX_TIMEOUT ]; then
                echo "timeout"
                break
        fi

        # worker crash
        ALIVE_WORKERS=$(pgrep -f "locust.*--worker" | wc -l)
        if [ "$ALIVE_WORKERS" -eq 0 ]; then
                echo "worker crashed"
                break
        fi

        sleep 5
done
echo "test all done"


# 결과 확인, 가상환경 및 docker 비활성화 후 종료
pkill -9 -f locust
pkill -9 -f "locust.*--worker"

sudo fuser -k 5557/tcp
sudo fuser -k 5558/tcp

sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches

deactivate
# redpanda 내 정상 적재 여부 확인
docker exec -it redpanda rpk group list
#docker logs redpanda > redpanda.log 2>&1 &
docker compose stop
#sudo rm -rf locust_env
docker compose rm -f redpanda
docker compose down -v
