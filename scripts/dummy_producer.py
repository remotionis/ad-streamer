import time
from locust import User, task, constant, events, LoadTestShape

class DummyUser(User):
    wait_time = constant(1) # 1초 휴식

    @task
    def do_nothing(self):
        start_time = time.time()
        # Kafka나 외부 통신 없이 그냥 허공에 대고 쏩니다 (Mocking)
        events.request.fire(
            request_type="Mock",
            name="Dummy_Traffic",
            response_time=(time.time() - start_time) * 1000,
            response_length=100,
            exception=None,
        )

# 기존과 동일한 파도형 트래픽 생성기
class SpikeTrafficShape(LoadTestShape):
    def tick(self):
        run_time = self.get_run_time()
        if run_time < 30: return (100, 20)
        elif run_time < 60: return (3000, 500)
        elif run_time < 90: return (100, 50)
        else: return None
