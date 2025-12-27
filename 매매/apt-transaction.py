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

# PublicDataReader API 컬럼명 (영어) → 한글 컬럼명 매핑
# 최신 버전의 PublicDataReader는 영어 컬럼명을 사용하므로 한글로 변환 필요
COLUMN_MAPPING = {
    'sggCd': '법정동시군구코드',
    'umdCd': '법정동읍면동코드',
    'landCd': '법정동지번코드',
    'bonbun': '법정동본번코드',
    'bubun': '법정동부번코드',
    'roadNm': '도로명',
    'roadNmSggCd': '도로명시군구코드',
    'roadNmCd': '도로명코드',
    'roadNmSeq': '도로명일련번호코드',
    'roadNmbCd': '도로명지상지하코드',
    'roadNmBonbun': '도로명건물본번호코드',
    'roadNmBubun': '도로명건물부번호코드',
    'umdNm': '법정동',
    'aptNm': '단지명',
    'jibun': '지번',
    'excluUseAr': '전용면적',
    'dealYear': '계약년도',
    'dealMonth': '계약월',
    'dealDay': '계약일',
    'dealAmount': '거래금액',
    'floor': '층',
    'buildYear': '건축년도',
    'aptSeq': '단지일련번호',
    'cdealType': '거래유형',
    'cdealDay': '해제사유발생일',
    'dealingGbn': '해제여부',
    'estateAgentSggNm': '중개사소재지',
    'rgstDate': '등기일자',
    'aptDong': '아파트동명',
    'slerGbn': '매도자',
    'buerGbn': '매수자',
    'landLeaseholdGbn': '토지임대부아파트여부',
    # 'buyerGbn': '매수자',  # buerGbn과 중복이므로 제외
}


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
    service_key, start_year_month, end_year_month, sigungu_codes, sigungu_code_df
):
    """
    모든 시군구에 대해 아파트 매매 실거래가 데이터를 수집합니다.
    노트북과 동일하게 각 시군구별 데이터 수집 시마다 즉시 시도명/시군구명을 병합합니다.

    Args:
        service_key (str): 공공데이터포털 API 인증키 (Decoding 된 일반 인증키)
        start_year_month (str): 시작 연월 (YYYYMM 형식)
        end_year_month (str): 종료 연월 (YYYYMM 형식)
        sigungu_codes (list): 시군구 코드 리스트
        sigungu_code_df (DataFrame): 시군구 코드 정보 데이터프레임

    Returns:
        list: 각 시군구별 데이터프레임을 담은 리스트 (시도명/시군구명이 이미 병합됨)
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

            # 데이터가 있으면 컬럼명 변환 후 시도명/시군구명 병합
            if data is not None and not data.empty:
                # 1. API가 반환한 영어 컬럼명을 한글로 변환
                data = data.rename(columns=COLUMN_MAPPING)

                # 2. 시도명/시군구명 컬럼 추가 (노트북과 동일한 방식)
                data = pd.merge(
                    data,
                    sigungu_code_df[["시도명", "시군구명", "시군구코드"]].drop_duplicates(
                        subset=["시군구코드"]
                    ),
                    left_on="법정동시군구코드",
                    right_on="시군구코드",
                    how="left",
                )

                # 3. 병합에 사용된 임시 컬럼 제거
                data = data.drop(columns=["시군구코드"])

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

    # 데이터프레임의 컬럼 확인
    print(f"사용 가능한 컬럼: {combined_data.columns.tolist()}")

    # 지역코드 컬럼명 찾기 (여러 가능성 체크)
    possible_code_columns = [
        "지역코드",
        "법정동시군구코드",
        "시군구코드",
        "법정동코드",
        "시도",
    ]

    code_column = None
    for col in possible_code_columns:
        if col in combined_data.columns:
            code_column = col
            print(f"지역 코드 컬럼으로 '{col}' 사용")
            break

    if code_column is None:
        print("⚠️  경고: 지역 코드 컬럼을 찾을 수 없습니다. 데이터를 그대로 반환합니다.")
        return combined_data

    # 시도명/시군구명 컬럼 추가
    try:
        merged_data = pd.merge(
            combined_data,
            sigungu_code[["시도명", "시군구명", "시군구코드"]].drop_duplicates(
                subset=["시군구코드"]
            ),
            left_on=code_column,
            right_on="시군구코드",
            how="left",
        )

        # 병합에 사용된 임시 컬럼 제거 (원본 컬럼과 다른 경우만)
        if code_column != "시군구코드":
            merged_data = merged_data.drop(columns=["시군구코드"])

        print(f"최종 데이터: {len(merged_data)}건")
        return merged_data

    except Exception as e:
        print(f"⚠️  병합 중 오류 발생: {str(e)}")
        print("데이터를 그대로 반환합니다.")
        return combined_data


def transform_data_columns(data):
    """
    데이터프레임의 컬럼명을 변경하고 순서를 조정합니다.

    Args:
        data (DataFrame): 원본 데이터프레임

    Returns:
        DataFrame: 변환된 데이터프레임
    """
    print("\n데이터 변환 중...")

    # 데이터 복사
    df = data.copy()

    # 1. '리' 컬럼 생성 및 초기화
    df['리'] = ''

    # 2. '법정동' 컬럼 값을 공백으로 분리하여 '법정동'과 '리' 컬럼에 할당
    def split_법정동(row):
        if '법정동' in row and pd.notna(row['법정동']):
            parts = str(row['법정동']).split()
            if len(parts) == 2:
                row['법정동'] = parts[0]
                row['리'] = parts[1]
        return row

    df = df.apply(split_법정동, axis=1)

    # 3. '계약일자' 컬럼 생성 (계약년도, 계약월, 계약일을 합쳐서 날짜 타입으로 변환)
    if all(col in df.columns for col in ['계약년도', '계약월', '계약일']):
        df['계약일자'] = pd.to_datetime(
            df['계약년도'].astype(str) + '-' +
            df['계약월'].astype(str).str.zfill(2) + '-' +
            df['계약일'].astype(str).str.zfill(2),
            errors='coerce'
        )

    # 4. 컬럼 이름 변경
    column_rename_map = {
        '시도명': '시도',
        '시군구명': '시군구',
        '단지명': '단지'
    }

    # 존재하는 컬럼만 변경
    rename_cols = {k: v for k, v in column_rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_cols)

    # 5. 열 순서 변경 (노트북과 동일한 순서)
    desired_column_order = [
        '시도', '시군구', '법정동', '리', '단지', '전용면적', '층', '건축년도',
        '계약년도', '계약월', '계약일', '계약일자', '거래금액', '거래유형',
        '법정동시군구코드', '법정동읍면동코드', '법정동지번코드', '법정동본번코드',
        '법정동부번코드', '도로명', '도로명시군구코드', '도로명코드', '도로명일련번호코드',
        '도로명지상지하코드', '도로명건물본번호코드', '도로명건물부번호코드', '지번',
        '단지일련번호', '해제여부', '해제사유발생일', '중개사소재지', '등기일자',
        '아파트동명', '매도자', '매수자', '토지임대부아파트여부'
    ]

    # 실제 존재하는 컬럼만 필터링
    available_columns = [col for col in desired_column_order if col in df.columns]

    # 원하는 순서에 없는 컬럼들 추가
    remaining_columns = [col for col in df.columns if col not in available_columns]
    final_column_order = available_columns + remaining_columns

    df = df[final_column_order]

    print(f"데이터 변환 완료: {len(df)}건")

    return df


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

    # 3. 모든 시군구에 대해 데이터 수집 (각 수집 시마다 시도명/시군구명 병합)
    all_data_list = collect_transaction_data(
        service_key=SERVICE_KEY,
        start_year_month=start_year_month,
        end_year_month=end_year_month,
        sigungu_codes=sigungu_codes,
        sigungu_code_df=sigungu_code,
    )

    # 4. 수집된 데이터가 없으면 종료
    if not all_data_list:
        print("\n⚠️  수집된 데이터가 없습니다.")
        return None

    # 5. 모든 데이터를 하나의 데이터프레임으로 병합
    print("\n데이터를 병합하는 중...")
    combined_data = pd.concat(all_data_list, ignore_index=True)
    print(f"병합 완료: {len(combined_data)}건의 거래 데이터")

    # 6. 데이터 변환 (컬럼명 변경 및 순서 조정)
    final_data = transform_data_columns(combined_data)

    print("\n" + "=" * 80)
    print("데이터 수집 완료!")
    print("=" * 80)
    print(f"\n총 거래 건수: {len(final_data):,}건")
    print(f"데이터 기간: {start_year_month} ~ {end_year_month}")
    print(f"\n데이터 컬럼 정보:")
    print(final_data.info())

    print("\n상위 5개 데이터:")
    print(final_data.head())

    # 7. CSV 파일로 저장
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)  # output 폴더가 없으면 생성

    # 파일명에 수집 기간 포함
    output_filename = f"아파트_매매_실거래가_{start_year_month}_{end_year_month}.csv"
    output_path = output_dir / output_filename

    final_data.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ CSV 파일 저장 완료: {output_path}")

    # 8. 데이터프레임 반환
    return final_data


if __name__ == "__main__":
    # 스크립트 실행
    df = main()

    # df 변수에 최종 데이터가 저장되어 있습니다
    # 이후 구글 시트 연동 코드에서 이 데이터를 활용할 수 있습니다
