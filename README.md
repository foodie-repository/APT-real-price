# 부동산 실거래가 데이터 수집 프로젝트

## 프로젝트 소개
부동산 실거래가 데이터를 수집하고 분석하는 프로젝트입니다.

## 기술 스택
- Python 3.11
- Jupyter Notebook
- pandas (데이터 처리)
- requests (API 호출)
- openpyxl (엑셀 파일 처리)
- PublicDataReader (국토교통부 API 연동)
- python-dotenv (환경변수 관리)

## 환경 설정

이 프로젝트는 `uv`를 사용하여 의존성을 관리합니다.

### 1. 의존성 설치
```bash
uv sync
```

### 2. API 인증키 설정

공공데이터포털에서 API 인증키를 발급받아 설정해야 합니다.

#### API 인증키 발급 방법
1. [공공데이터포털](https://www.data.go.kr/) 회원가입
2. '국토교통부 아파트매매 실거래 상세 자료' 검색
3. 활용신청 → 일반 인증키(Decoding) 발급

#### .env 파일 생성
```bash
# .env.example 파일을 복사하여 .env 파일 생성
cp .env.example .env
```

#### .env 파일 편집
```bash
# .env 파일을 열어서 발급받은 API 키 입력
PUBLIC_DATA_SERVICE_KEY=your-api-key-here
```

**⚠️ 중요: .env 파일은 Git에 커밋되지 않습니다 (.gitignore에 추가됨)**

### 3. 가상환경 활성화
```bash
# macOS/Linux
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 4. Jupyter Notebook 실행
```bash
# uv를 사용하여 Jupyter 실행
uv run jupyter notebook

# 또는 가상환경 활성화 후
jupyter notebook
```

## 프로젝트 구조
```
실거래가/
├── .venv/                          # 가상환경 (자동 생성)
├── .env                            # API 키 설정 (Git 제외)
├── .env.example                    # API 키 설정 템플릿
├── .gitignore                      # Git 무시 파일
├── pyproject.toml                  # 프로젝트 설정 및 의존성
├── .python-version                 # Python 버전 (3.11)
├── main.py                         # 메인 실행 파일
├── README.md                       # 프로젝트 문서
├── Archive/                        # 참고용 노트북
│   └── 실거래가 수집 (매매).ipynb
└── 매매/                           # 매매 실거래가 데이터 수집
    ├── apt-transaction.py          # 아파트 매매 데이터 수집 스크립트
    └── output/                     # CSV 출력 폴더 (자동 생성)
```

## 주요 명령어

### uv 명령어
```bash
# 의존성 추가
uv add <패키지명>

# 의존성 제거
uv remove <패키지명>

# 의존성 동기화
uv sync

# Python 스크립트 실행
uv run python main.py

# Jupyter Notebook 실행
uv run jupyter notebook
```

## 사용 방법

### 아파트 매매 실거래가 데이터 수집

최근 4개월간의 전국 아파트 매매 실거래가 데이터를 수집합니다.

```bash
# 스크립트 실행
uv run python 매매/apt-transaction.py
```

실행하면 다음 작업이 자동으로 수행됩니다:
1. 전국 시군구 코드 정보 로드
2. 최근 4개월 기간 자동 계산
3. 모든 시군구에 대해 데이터 수집
4. 시도명/시군구명 정보 추가
5. CSV 파일로 저장 (`매매/output/아파트_매매_실거래가_YYYYMM_YYYYMM.csv`)
6. pandas DataFrame으로 반환

### Jupyter Notebook 작업

```bash
# Jupyter Notebook 실행
uv run jupyter notebook
```

## 개발 가이드

### 새 패키지 추가하기
```bash
# 예: matplotlib 추가
uv add matplotlib
```

### API 키 관리
- `.env` 파일에 실제 API 키 저장 (Git에 커밋 안 됨)
- `.env.example` 파일은 템플릿으로 Git에 커밋됨
- 새 환경에서 작업 시: `cp .env.example .env` 후 키 입력

## 참고 자료
- [uv 공식 문서](https://github.com/astral-sh/uv)
- [pandas 문서](https://pandas.pydata.org/)
- [Jupyter 문서](https://jupyter.org/)
- [PublicDataReader 문서](https://github.com/WooilJeong/PublicDataReader)
- [공공데이터포털](https://www.data.go.kr/)
