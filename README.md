[토이 프로젝트] Ad-Streamer

## 요약
1. **목적**: 대규모 트래픽 발생 환경에서 안정적으로 동작 가능한 데이터 처리 파이프라인 설계 연습
2. **프로젝트 테마**: *"광고 노출 및 클릭에 의해 발생하는 대규모 로그의 실시간 처리 시스템 구축 및 최적화"*
3. **목표 처리 규모**
- 평균 데이터 유입량: 초당 약 500~1,000 건
- 피크 타임 가정 시 최대 유입량: 초당 약 3,000~5,000 건

---
## 인프라 및 기술 스택
### 1. 인프라
- Google Cloud의 무료 체험 제공 자원 사용  
>- 인프라: GCP e2-standard-8 1대
>- 스토리지: GCS(Google Cloud Storage) + Delta Lake 4.0.  
- 단일 VM 인스턴스 내 전체 파이프라인 구현 목표  

### 2. 단계별 기술 스택 및 데이터 포맷
1. 


---
## 로그(데이터) 스키마 명세
- **event_id**(String): 로그 고유 식별자(Private Key)
- **user_id**(String): 익명화된 가상 유저 식별 ID
- **ad_id**(String): 광고 캠페인/소재 ID
- **event_type**(String): 노출 / 클릭 구분
- **site_url**(String): 접근 경로(광고가 노출된 웹페이지 주소)
- **device_os**(String): 기기 정보(ios, android, pc 등)
- **ip_address**(String): 사용자 IP (위치 추정 및 봇 탐지용)
- **timestamp**(Timestamp/ISO 8601): 이벤트 발생 시각


---
## History
- 6/5 ~ 6/6   프로젝트 구상 및 세부 아키텍처 설계
- 6/7 ~ 6/    Google Cloud 환경 설정, VM 인스턴스 내 docker 환경 및 Git 셋업
- 
