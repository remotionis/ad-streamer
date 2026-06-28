# test_kafka.py
from confluent_kafka import Producer
import json

print("🚀 1. Kafka Producer 초기화를 시도합니다...")
# 타임아웃을 3초로 짧게 주어 무한 대기를 방지합니다.
p = Producer({
    'bootstrap.servers': '127.0.0.1:19092',
    'socket.timeout.ms': 3000,
    'message.timeout.ms': 3000
})
print("✅ 1. 초기화 성공!")

print("🚀 2. 테스트 메시지 전송을 시도합니다...")
p.produce(
    topic='ad-stream-topic', 
    key='test_user', 
    value=json.dumps({"msg": "hello redpanda"}).encode('utf-8')
)
print("✅ 2. 전송 큐(Queue) 삽입 성공!")

print("🚀 3. Redpanda로 데이터를 완전히 밀어 넣습니다 (Flush)...")
# 최대 5초간 대기하며 전송 완료를 기다립니다.
remaining = p.flush(timeout=5)

if remaining == 0:
    print("🎉 [성공] Redpanda와 완벽하게 통신이 완료되었습니다!")
else:
    print(f"❌ [실패] {remaining}개의 메시지가 전송되지 못하고 큐에 남아있습니다. 통신 장애입니다.")
