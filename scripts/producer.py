# locust로 데이터 생성 -> Kafka로 전송
import json
import time
import yaml
from locust import User, task, events, LoadTestShape
import locust.runners
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from data_generator import generate_ad_log


# config 세팅값 가져오기 + 셋업
with open('config.yaml', 'r', encoding='utf-8') as file:
    data = yaml.safe_load(file)

KAFKA_BROKER = data['broker']['external']
TOPIC_NAME = data['topic']


# kafka 토픽 생성 - 동일 토픽명 존재 시 기존 토픽 삭제 후 수행
def reset_kafka_topic():
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})

    # 현재 토픽 목록 확인
    metadata = admin_client.list_topics(timeout=10)

    # 기존 토픽 확인 후 제거
    if TOPIC_NAME in metadata.topics:
        try:
            fs = admin_client.delete_topics([TOPIC_NAME])
            for topic, f in fs.items():
                f.result()
            print("기존 토픽 발견, 제거 완료")
        except Exception as e:
            print(f"토픽 제거 중 에러: {e}")
    time.sleep(2)

    try:
        new_topic = NewTopic(TOPIC_NAME, num_partitions=10, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            f.result()
        print("신규 토픽 생성 완료")
    except Exception as e:
        print(f"토픽 생성 중 에러: {e}")


# 이벤트 훅 - 테스트 시 reset_kafka_topic은 최초 1회만 실행되도록
@events.test_start.add_listener
def before_start(environment, **kwargs):
    if not isinstance(environment.runner, locust.runners.WorkerRunner):
        reset_kafka_topic()


# Kafka Producer 초기화
producer = Producer({
    'bootstrap.servers': KAFKA_BROKER,
    'queue.buffering.max.messages': 100000, # 버퍼 크기
    'linger.ms': 5, # 대기시간(의도적 지연) - 미니 배치 형성될 만큼 데이터 쌓아둠
    'message.timeout.ms': 10000, # 10초 내에 메시지 못 가면 drop
    'compression.type': 'lz4' # 데이터 압축 -> 네트워크 대역폭 절약
})


# 가상 유저
class KafkaUser(User):
    abstract = False

    @task
    def send_ad_log(self):
        start_time = time.time()
        log_data = generate_ad_log()

        try:
            # Kafka로 JSON 데이터 전송(비동기) 시도
            producer.produce(
                topic=TOPIC_NAME,
                key=log_data["user_id"], # 파티션 키
                value=json.dumps(log_data).encode('utf-8')
            )
            # 성공 시: Locust UI 및 통계에 기록
            events.request.fire(
                request_type="Kafka",
                name="Produce_Ad_Log",
                response_time=(time.time() - start_time) * 1000,
                response_length=len(str(log_data)),
                exception=None,
            )
        except Exception as e:
            # 실패 시: Locust에 에러 기록
            events.request.fire(
                request_type="Kafka",
                name="Produce_Ad_Log",
                response_time=(time.time() - start_time) * 1000,
                response_length=0,
                exception=e,
            )
        producer.poll(0) # callback


# 커스텀 스케줄러
# 시간에 따른 트래픽 변화 발생
class SpikeTrafficShape(LoadTestShape):
    def tick(self):
        run_time = self.get_run_time()

        # 0 ~ 30초: 목표 유저 100명, 초당 20건 생성
        if run_time < 30:
            return (100, 20)

        # 30초 ~ 60초: 피크 타임 폭주 - 목표 3000명, 초당 500
        elif run_time < 60:
            return (3000, 500)

        # 60초 ~ 90초: 0초와 동일
        elif run_time < 90:
            return (100, 50)

        # > 90초: 테스트 종료
        else:
            producer.flush()
            return None
