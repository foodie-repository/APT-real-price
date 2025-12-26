"""
아파트 매매 실거래가 데이터 수집 스크립트

이 스크립트는 국토교통부 실거래가 공개시스템 API를 통해
최근 4개월간의 아파트 매매 실거래가 데이터를 수집합니다.

필요 라이브러리:
- PublicDataReader
- pandas
- python-dateutil
"""

import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd
import PublicDataReader as pdr
from PublicDataReader import TransactionPrice
from dotenv import load_dotenv

# .env 파일 로드
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)


def get_date_range(months_back=4):
    """
    현재 날짜를 기준으로 데이터 수집 기간을 계산합니다.

    Args:
        months_back (int): 과거 몇 개월까지 데이터를 수집할지 지정 (기본값: 4개월)

    Returns:
        tuple: (start_year_month, end_year_month) 형식의 문자열 튜플
                예: ('202408', '202412')
    """
    # 현재 날짜
    current_date = datetime.now()

    # 종료 월: 현재 월
    end_year_month = current_date.strftime("%Y%m")

    # 시작 월: N개월 전
    start_date = current_date - relativedelta(months=months_back - 1)
    start_year_month = start_date.strftime("%Y%m")

    print(f"데이터 수집 기간: {start_year_month} ~ {end_year_month}")

    return start_year_month, end_year_month


def get_sigungu_codes():
    """
    전국 시군구 코드 정보를 가져옵니다.

    Returns:
        DataFrame: 시군구 코드 정보가 담긴 데이터프레임
    """
    print("시군구 코드 정보를 가져오는 중...")
    sigungu_code = pdr.code_bdong()
    print(f"총 {len(sigungu_code['시군구코드'].unique())}개 시군구 발견")
    return sigungu_code


def collect_transaction_data(
    service_key, start_year_month, end_year_month, sigungu_codes
):
    """
    모든 시군구에 대해 아파트 매매 실거래가 데이터를 수집합니다.

    Args:
        service_key (str): 공공데이터포털 API 인증키 (Decoding 된 일반 인증키)
        start_year_month (str): 시작 연월 (YYYYMM 형식)
        end_year_month (str): 종료 연월 (YYYYMM 형식)
        sigungu_codes (list): 시군구 코드 리스트

    Returns:
        list: 각 시군구별 데이터프레임을 담은 리스트
    """
    # TransactionPrice API 초기화
    api = TransactionPrice(service_key)

    # 데이터를 저장할 리스트
    all_data_list = []

    total_codes = len(sigungu_codes)
    print(f"\n총 {total_codes}개 시군구의 데이터를 수집합니다...\n")

    # 각 시군구별로 데이터 수집
    for idx, code in enumerate(sigungu_codes, 1):
        try:
            print(f"[{idx}/{total_codes}] 시군구 코드 {code} 데이터 수집 중...")

            # API를 통해 데이터 가져오기
            data = api.get_data(
                property_type="아파트",
                trade_type="매매",
                sigungu_code=code,
                start_year_month=start_year_month,
                end_year_month=end_year_month,
            )

            # 데이터가 있으면 리스트에 추가
            if data is not None and not data.empty:
                all_data_list.append(data)
                print(f"  → {len(data)}건의 거래 데이터 수집 완료")
            else:
                print(f"  → 데이터 없음")

        except Exception as e:
            print(f"  → 오류 발생: {str(e)}")
            continue

    print(f"\n데이터 수집 완료! 총 {len(all_data_list)}개 시군구에서 데이터 수집")

    return all_data_list


def merge_data_with_region_info(combined_data, sigungu_code):
    """
    수집된 데이터에 시도명/시군구명 정보를 추가합니다.

    Args:
        combined_data (DataFrame): 병합된 거래 데이터
        sigungu_code (DataFrame): 시군구 코드 정보

    Returns:
        DataFrame: 지역 정보가 추가된 데이터프레임
    """
    print("\n지역 정보를 추가하는 중...")

    # 시도명/시군구명 컬럼 추가
    merged_data = pd.merge(
        combined_data,
        sigungu_code[["시도명", "시군구명", "시군구코드"]].drop_duplicates(
            subset=["시군구코드"]
        ),
        left_on="법정동시군구코드",
        right_on="시군구코드",
        how="left",
    )

    # 병합에 사용된 임시 컬럼 제거
    merged_data = merged_data.drop(columns=["시군구코드"])

    print(f"최종 데이터: {len(merged_data)}건")

    return merged_data


def main():
    """
    메인 실행 함수

    사용 전 준비사항:
    1. 공공데이터포털(https://www.data.go.kr/)에서 회원가입
    2. '국토교통부 아파트매매 실거래 상세 자료' API 신청
    3. 발급받은 일반 인증키(Decoding)를 .env 파일에 입력
    """

    # ========================================
    # API 인증키 설정 (필수!)
    # ========================================
    # .env 파일에서 API 인증키를 불러옵니다
    SERVICE_KEY = os.getenv("PUBLIC_DATA_SERVICE_KEY", "")

    if not SERVICE_KEY:
        print("=" * 80)
        print("⚠️  오류: API 인증키가 설정되지 않았습니다!")
        print("=" * 80)
        print("\n.env 파일 설정 방법:\n")
        print("1. 프로젝트 루트 디렉토리에 .env 파일 생성:")
        print("   cp .env.example .env")
        print("\n2. .env 파일을 열어서 API 인증키 입력:")
        print("   PUBLIC_DATA_SERVICE_KEY=your-api-key-here")
        print("\n" + "=" * 80)
        print("\nAPI 인증키 발급 방법:")
        print("1. 공공데이터포털(https://www.data.go.kr/) 회원가입")
        print("2. '국토교통부 아파트매매 실거래 상세 자료' 검색")
        print("3. 활용신청 → 일반 인증키(Decoding) 발급")
        print("4. 발급받은 키를 .env 파일에 입력")
        print("=" * 80)
        return None

    print("=" * 80)
    print("아파트 매매 실거래가 데이터 수집 시작")
    print("=" * 80)

    # 1. 데이터 수집 기간 계산 (최근 4개월)
    start_year_month, end_year_month = get_date_range(months_back=4)

    # 2. 시군구 코드 정보 가져오기
    sigungu_code = get_sigungu_codes()
    sigungu_codes = sigungu_code["시군구코드"].unique().tolist()

    # 3. 모든 시군구에 대해 데이터 수집
    all_data_list = collect_transaction_data(
        service_key=SERVICE_KEY,
        start_year_month=start_year_month,
        end_year_month=end_year_month,
        sigungu_codes=sigungu_codes,
    )

    # 4. 수집된 데이터가 없으면 종료
    if not all_data_list:
        print("\n⚠️  수집된 데이터가 없습니다.")
        return None

    # 5. 모든 데이터를 하나의 데이터프레임으로 병합
    print("\n데이터를 병합하는 중...")
    combined_data = pd.concat(all_data_list, ignore_index=True)
    print(f"병합 완료: {len(combined_data)}건의 거래 데이터")

    # 6. 시도명/시군구명 정보 추가
    final_data = merge_data_with_region_info(combined_data, sigungu_code)

    # 7. 데이터 정보 출력
    print("\n" + "=" * 80)
    print("데이터 수집 완료!")
    print("=" * 80)
    print(f"\n총 거래 건수: {len(final_data):,}건")
    print(f"데이터 기간: {start_year_month} ~ {end_year_month}")
    print(f"\n데이터 컬럼 정보:")
    print(final_data.info())

    print("\n상위 5개 데이터:")
    print(final_data.head())

    # 8. 데이터프레임 반환 (구글 시트 연동 시 사용)
    return final_data


if __name__ == "__main__":
    # 스크립트 실행
    df = main()

    # df 변수에 최종 데이터가 저장되어 있습니다
    # 이후 구글 시트 연동 코드에서 이 데이터를 활용할 수 있습니다
