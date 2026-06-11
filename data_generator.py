# locust 랜덤 데이터 제조 코드
import random
import uuid
from datetime import datetime, timezone

# 가상 URL 패턴
CATEGORIES = ["news", "cafe", "blog"]
DOMAINS = ["naver.com", "daum.net"]
PATHS = ["/main", "/article/view", "/board/list", "/sports/today", "/"]
QUERY_PARAMS = ["?id=123", "?category=IT", "?page=1", ""]

# 기기 정보
DEVICE_TYPES = ["pc", "ios", "android", "tablet"]

def generate_ad_log():
    """1건의 가상 광고 로그(JSON)를 생성하는 함수"""
    
    # URL 조합 (도메인 + 경로 + 쿼리파라미터)
    site_url = f"https://{random.choice(CATEGORIES)}.{random.choice(DOMAINS)}{random.choice(PATHS)}{random.choice(QUERY_PARAMS)}"
    
    # 노출 빈도가 클릭 대비 훨씬 많은 점을 고려 -> 98:2로 비율 설정
    event_type = random.choices(["impression", "click"], weights=[98, 2], k=1)[0]
    
    log = {
        "event_id": str(uuid.uuid4()), # 로그 primary key
        "user_id": f"user_{random.randint(1, 10000000)}", # 가상 이용자
        "ad_id": f"cam_{random.randint(1, 100000)}", # 광고 ID
        "event_type": event_type, # 노출 or 클릭
        "site_url": site_url, # 접근 경로
        "device_os": random.choice(DEVICE_TYPES), # 기기 정보
        "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        "timestamp": datetime.now(timezone.utc).isoformat() # ISO 8601 타임스탬프
    }
    
    return log
