# locust로 데이터 생성 -> Kafka로 전송
import json
import time
import yaml
from locust import User, task, events, LoadTestShape, constant
from confluent_kafka import Producer
from data_generator import generate_ad_log

print("producer start")
# config 세팅값 가져오기 + 셋업
with open('./config.yaml', 'r', encoding='utf-8') as file:
    data = yaml.safe_load(file)

KAFKA_BROKER = data['broker']['external']
TOPIC_NAME = data['topic']
print(f"{KAFKA_BROKER}, {TOPIC_NAME}")

producer = None

def get_producer():
    global producer
    if producer is None:
        print("producer creating", flush=True)
        producer = Producer({
            'bootstrap.servers': KAFKA_BROKER,
            'queue.buffering.max.messages': 100000, # 버퍼 크기
            'linger.ms': 5, # 대기시간(의도적 지연) - 미니 배치 형성될 만큼 데이터 쌓아둠
            'message.timeout.ms': 10000, # 10초 내에 메시지 못 가면 drop
            'compression.type': 'lz4', # 데이터 압축 -> 네트워크 대역폭 절약
        })
        print("created", flush=True)
    return producer

# 가상 유저
class KafkaUser(User):
    abstract = False
    wait_time = constant(1)

    @task
    def send_ad_log(self):
        start_time = time.time()
        log_data = generate_ad_log()

        p = get_producer()

        try:
            # Kafka로 JSON 데이터 전송(비동기) 시도
            p.produce(
                topic=TOPIC_NAME,
                key=log_data["user_id"], # 파티션 키
                value=json.dumps(log_data).encode('utf-8')
            )
            print(f"pd-{log_data['user_id']}", flush=True)

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
            print(e)
        finally:
            if p:
                p.poll(0) # callback

@events.test_stop.add_listener
def test_stop(environment, **kwargs):
    if not environment.parsed_options.master and producer:
        producer.flush(timeout=5)
        print("test stop", flush=True)


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
            return None

