# -*- coding: utf-8 -*-
"""
ETL (Extract, Transform, Load) 모듈

[역할]
- 원본 CSV 파일에서 데이터를 읽어서(Extract)
- 분석하기 좋은 형태로 변환하고 정제한 뒤(Transform)
- 판다스 데이터프레임(DataFrame) 객체로 반환함.
- DB에 적재(Load)하기 전의 모든 데이터 전처리를 담당함.

[주요 기능]
1. CSV 파일 읽기 (한글 깨짐 방지를 위한 인코딩 처리 포함)
2. Wide 포맷 → Long 포맷으로 변환 (데이터를 다루기 쉬운 형태로 변경)
3. 날짜 형식 통일 (예: "2017.1" → "2017-01")
4. 지역명 통일 및 지역 코드로 변환 (예: "서울" → 11)
5. 단위 변환 (예: 천명 → 명)
6. 불필요한 데이터나 결측치(값이 없는 경우) 처리

[데이터 기간]
- 기존 데이터: 2017-2025 (실업률, 산업별 고용, 인구)
- 신규 데이터: 2017-2025 (고용보험, 교육수준/연령대별 고용 등)
"""

# --- 기본 라이브러리 임포트 ---
from pathlib import Path  # 파일 경로를 쉽게 다루기 위한 라이브러리
from typing import Dict, Tuple  # 타입 힌팅을 위한 라이브러리 (코드의 가독성을 높여줌)
import logging  # 로그 기록용 라이브러리

import pandas as pd  # 데이터 분석의 핵심! 판다스(Pandas) 라이브러리. 보통 pd라는 별명으로 사용함.

# --- 로깅 설정 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --- 경로 설정 ---
# 이 파일(etl.py)이 있는 위치를 기준으로 프로젝트의 최상위 폴더 경로를 찾음.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
# 데이터가 들어있는 폴더들의 경로를 미리 지정해둠.
DATA_DIR = PROJECT_ROOT / "data" / "raw"          # 기존 CSV 파일들이 있는 곳
NEW_DATA_DIR = PROJECT_ROOT / "data" / "new_data"  # 새로 추가된 CSV 파일들이 있는 곳

# --- 지역명-지역코드 매핑 테이블 ---
# "서울특별시" 같은 한글 이름 대신 11 같은 숫자 코드를 사용하기 위해 미리 정의해둠.
# 숫자 코드를 쓰면 나중에 데이터를 합치거나(join) 검색할 때 훨씬 빠르고 효율적임.
# 이 코드는 행정안전부에서 사용하는 표준 지역 코드임.
REGION_CODE_MAP: Dict[str, int] = {
    "서울특별시": 11, "부산광역시": 26, "대구광역시": 27, "인천광역시": 28,
    "광주광역시": 29, "대전광역시": 30, "울산광역시": 31, "세종특별자치시": 36,
    "경기도": 41, "강원도": 42, "충청북도": 43, "충청남도": 44,
    "전라북도": 45, "전라남도": 46, "경상북도": 47, "경상남도": 48,
    "제주특별자치도": 50,
    # 새로운 지역명 추가 (특별자치도 개편)
    "강원특별자치도": 42,  # 강원도와 동일한 코드
    "전북특별자치도": 45,  # 전라북도와 동일한 코드
}


def extract_unemployment() -> pd.DataFrame:
    """
    실업률 관련 데이터를 CSV 파일에서 읽어와 정제함.

    [입력 파일]
    - data/raw/unemployment.csv (통계청 지역별 고용조사 데이터)

    [주요 처리 과정]
    1. Wide 포맷 → Long 포맷으로 변환: 컬럼으로 펼쳐져 있던 지역들을 행으로 내림.
    2. 날짜 형식 통일: "2017.1" 같은 형식을 "2017-01"로 바꿈. (정렬 및 비교를 쉽게 하기 위함)
    3. 단위 변환: '천명' 단위의 데이터를 '명' 단위로 바꿈. (1000을 곱함)
    4. 지역명을 표준 지역 코드로 변환.
    5. Long 포맷 → 다시 Wide 포맷으로 변환: '실업률', '취업자' 등 각 항목을 별도의 컬럼으로 만듦.
    
    [최종 출력 컬럼]
    - region_id: 지역 코드 (예: 11)
    - year_month: 연월 (예: "2017-01")
    - unemployment_rate: 실업률 (%)
    - unemployment_level: 실업자 수 (명)
    - labor_force: 경제활동인구 (명)
    - employed_persons: 취업자 수 (명)
    """

    csv_path = DATA_DIR / "unemployment.csv"
    # CSV 파일을 읽어 데이터프레임으로 만듦. 한글 깨짐 방지를 위해 encoding='utf-8-sig' 사용.
    df = pd.read_csv(csv_path, encoding="utf-8-sig", dtype={"시점": str})
    logger.info(f"✓ 실업률 원본 로드: {df.shape}") # (행, 열) 개수 출력

    # [핵심] Wide 포맷 → Long 포맷으로 변환 (pd.melt 사용)
    # 원본: | 시점 | 항목 | 서울 | 부산 | ... |  <- 지역이 옆으로 쭉 펼쳐져 있음 (Wide)
    # 변환: | 시점 | 항목 | region_name | value | <- 지역이 'region_name' 컬럼 아래로 쌓임 (Long)
    # 이렇게 바꿔야 지역별로 그룹핑하거나 필터링하기 편해짐.
    id_vars = ["시점", "항목"]  # 형태를 유지할 고정 컬럼
    region_cols = [c for c in df.columns if c not in id_vars]  # 행으로 내릴 대상이 되는 지역 컬럼들
    tidy = df.melt(id_vars=id_vars, value_vars=region_cols, var_name="region_name", value_name="value")

    # 날짜 형식 통일: "2017.1" → "2017-01"
    # DB에서 날짜를 문자열로 다룰 때, "2017-1"은 "2017-11"보다 뒤에 정렬되는 문제가 있음.
    # 월(month)을 항상 두 자리(01, 02, ...)로 맞춰주면 이런 문제가 해결됨.
    date_parts = tidy["시점"].astype(str).str.strip().str.split(".", expand=True)
    tidy["year_month"] = date_parts[0] + "-" + date_parts[1].str.zfill(2)  # zfill(2): 2자리를 만들고 빈 곳은 0으로 채움

    # 지역명 및 항목명 정리 (양쪽 공백 제거, 이름 통일 등)
    # CSV 파일마다 '제주도', '제주특별자치도' 처럼 표기가 다를 수 있어서 하나로 통일시킴.
    tidy["region_name"] = tidy["region_name"].str.strip().replace({"제주도": "제주특별자치도"})
    tidy["metric"] = tidy["항목"].str.strip()
    tidy["value"] = pd.to_numeric(tidy["value"], errors="coerce")  # 숫자여야 할 'value' 컬럼을 숫자로 변환. 변환 실패 시 NaN(결측치)으로 만듦.

    # 단위 통일: 천명 → 명 (1,000 곱하기)
    # 통계청 데이터는 보통 큰 숫자를 '천명' 단위로 제공하므로, 실제 인원수로 변환해줌.
    thousand_metrics = {"경제활동인구 (천명)", "취업자 (천명)", "실업자 (천명)", "15세이상인구 (천명)"}
    # isin()을 사용해 위 항목들에 해당하는 행을 찾고, 그 행들의 'value'에만 1000을 곱함.
    tidy.loc[tidy["metric"].isin(thousand_metrics), "value"] *= 1_000

    # [핵심] Long 포맷 → 다시 Wide 포맷으로 변환 (pivot_table 사용)
    # DB 테이블 구조에 맞추기 위해, 각 항목('실업률', '취업자' 등)을 별도의 컬럼으로 만듦.
    # Long: | region | year_month | metric          | value |
    #       | 서울   | 2017-01    | 실업률 (%)       | 3.5   |
    #       | 서울   | 2017-01    | 취업자 (천명)   | 5000  |
    # Wide: | region | year_month | unemployment_rate | employed_persons | ... |
    #       | 서울   | 2017-01    | 3.5               | 5000000          | ... |
    pivot = tidy.pivot_table(
        index=["region_name", "year_month"],  # 새로 만들 데이터프레임의 행 인덱스가 될 컬럼들
        columns="metric",                      # 컬럼으로 만들 값이 들어있는 컬럼
        values="value",                        # 새로 생긴 컬럼들을 채울 값이 들어있는 컬럼
        aggfunc="first"                        # 중복된 값이 있을 경우 첫 번째 값을 사용
    ).reset_index() # pivot_table을 하면 index로 지정된 컬럼들이 인덱스가 되므로, 다시 일반 컬럼으로 빼줌.

    # 컬럼명을 한글 → 영어로 변경
    # DB에서 SQL 쿼리를 작성할 때 한글 컬럼명은 불편할 수 있으므로 영어로 통일함.
    pivot = pivot.rename(columns={
        "실업률 (%)": "unemployment_rate",
        "실업자 (천명)": "unemployment_level",
        "경제활동인구 (천명)": "labor_force",
        "취업자 (천명)": "employed_persons",
    })

    # 지역명을 지역 코드로 변환 (map 사용)
    # 위에서 정의한 REGION_CODE_MAP을 사용해 "서울특별시" → 11, "부산광역시" → 26 등으로 바꿈.
    pivot["region_id"] = pivot["region_name"].map(REGION_CODE_MAP)
    # '전국', '계' 처럼 지역 코드가 없는 행(NaN)은 분석에 불필요하므로 제거함.
    pivot = pivot.dropna(subset=["region_id"])
    pivot["region_id"] = pivot["region_id"].astype(int)  # 숫자 코드는 정수(int) 타입으로 변경.

    logger.info(f"✓ 실업률 데이터 정제 완료: {len(pivot)}행, {pivot['region_id'].nunique()}개 지역")

    # 최종적으로 DB 테이블에 필요한 컬럼들만 선택하여 반환.
    return pivot[["region_id", "year_month", "unemployment_rate", "unemployment_level", "labor_force", "employed_persons"]]


def extract_employment() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    산업별 고용 데이터를 추출하고 정제함.

    [입력 파일]
    - data/raw/employment_industry.csv

    [처리 과정]
    1. '취업자' 데이터만 필터링.
    2. Wide → Long 포맷으로 변환.
    3. 산업 코드와 산업명을 분리 (정규표현식 사용).
    4. Fact 테이블과 Dimension 테이블로 분리.
       - Fact: 실제 측정값(취업자 수)이 담긴 테이블.
       - Dimension: 측정값의 기준이 되는 정보(산업 코드, 산업명)가 담긴 테이블.
       - 이렇게 분리하면 데이터 중복을 줄이고 구조를 더 명확하게 할 수 있음 (정규화).

    [출력]
    1) fact_employment (팩트 테이블): (region_id, industry_code, year_month, employed_persons)
    2) dim_industry (차원 테이블): (industry_code, industry_name)
    """

    csv_path = DATA_DIR / "employment_industry.csv"
    # 이 파일은 cp949(윈도우 기본 한글 인코딩)으로 저장되어 있어 cp949로 읽음.
    df = pd.read_csv(csv_path, encoding="cp949")
    logger.info(f"✓ 산업별 고용 원본 로드: {df.shape}")

    # 컬럼명에 포함된 불필요한 공백이나 문자를 제거.
    df.columns = [c.strip().replace(" 월", "") for c in df.columns]
    value_cols = [c for c in df.columns if c not in {"시도별", "산업별", "항목", "단위"}]

    # '항목' 컬럼에서 '취업자'가 포함된 행, '단위' 컬럼에서 '천명'이 포함된 행만 남김.
    df = df[df["항목"].astype(str).str.contains("취업자") & df["단위"].astype(str).str.contains("천명")]

    # Wide → Long 변환
    tidy = df.melt(
        id_vars=["시도별", "산업별"],
        value_vars=value_cols,
        var_name="year_month",
        value_name="employed"
    )

    # 데이터 정제
    tidy["year_month"] = tidy["year_month"].str.replace(".", "-", regex=False)
    tidy["region_name"] = tidy["시도별"].astype(str).str.strip().replace({"제주도": "제주특별자치도"})
    tidy["region_id"] = tidy["region_name"].map(REGION_CODE_MAP)
    tidy["employed"] = pd.to_numeric(tidy["employed"], errors="coerce") * 1_000 # 천명 -> 명

    # 산업 코드와 산업명 분리 (정규표현식 사용)
    # "A 농업, 임업 및 어업" -> "A"와 "농업, 임업 및 어업"으로 분리.
    tidy["industry_raw"] = tidy["산업별"].astype(str).str.strip()
    industry_info = tidy["industry_raw"].str.extract(r"^(?P<industry_code>[A-Z])\s+(?P<industry_name>.+)$")

    # ★ 데이터 품질 개선: 대분류 코드(A-U)만 사용하여 중복 집계 방지
    # 통계청 원천 데이터는 대분류(A, C, F...)와 업종명(광공업, 제조업...) 분류가 혼재되어 있어,
    # 단순 합계 시 동일 취업자가 2배로 집계되는 문제가 있음.
    # 따라서 한국표준산업분류(KSIC) 대분류 코드만 선택하여 상호 배타적인 분류 체계를 유지함.
    VALID_INDUSTRY_CODES = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I',
                            'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U']

    tidy = pd.concat([tidy, industry_info], axis=1) # 원래 데이터와 분리한 산업 정보를 합침.

    # 대분류 코드가 있는 행만 필터링 (중복 업종명 제거)
    tidy = tidy[tidy["industry_code"].isin(VALID_INDUSTRY_CODES)]

    # Fact 테이블 생성
    fact = tidy.dropna(subset=["region_id", "industry_code", "year_month", "employed"])
    fact = fact[["region_id", "industry_code", "year_month", "employed"]].rename(columns={"employed": "employed_persons"})
    fact["region_id"] = fact["region_id"].astype(int)

    # Dimension 테이블 생성
    dim_industry = (
        tidy[["industry_code", "industry_name"]]
        .dropna(subset=["industry_code"]) # 코드가 없는 경우는 제외
        .drop_duplicates() # 중복 제거
        .sort_values("industry_code") # 코드로 정렬
    )

    logger.info(f"✓ 산업별 고용 데이터 정제 완료: {len(fact)}행, {dim_industry['industry_code'].nunique()}개 산업")

    return fact, dim_industry


def extract_population() -> pd.DataFrame:
    """
    월별 인구 데이터를 추출하고 정제함.

    [입력 파일] data/raw/population.csv
    [출력] 월별 인구 데이터 (region_id, year_month, total_pop)
    """

    csv_path = DATA_DIR / "population.csv"
    # 이 CSV는 헤더(컬럼명)가 2줄로 되어 있어 header=[0, 1]로 지정.
    df = pd.read_csv(csv_path, encoding="utf-8-sig", header=[0, 1])
    logger.info(f"✓ 인구 원본 로드: {df.shape}")

    # 2중으로 된 컬럼명을 정리. (예: ('2017.01', '총인구수 (명)'))
    df.columns = pd.MultiIndex.from_tuples(
        [(str(c[0]).strip(), str(c[1]).strip()) for c in df.columns],
        names=["year_month_raw", "metric"]
    )

    # 지역명 컬럼을 따로 빼두고, '전국' 행은 제외.
    region_series = df[("행정구역(시군구)별", "행정구역(시군구)별")].astype(str).str.strip()
    mask = region_series != "전국"
    region_series = region_series[mask]
    df = df.loc[mask].drop(columns=[("행정구역(시군구)별", "행정구역(시군구)별")])

    # Wide -> Long 변환 (for loop 사용)
    # 각 월별 컬럼을 순회하면서 Long 형태로 데이터를 쌓음.
    monthly = []
    for month_raw, metric in df.columns:
        month_clean = month_raw.replace(".", "-")
        temp = pd.DataFrame({
            "region_name": region_series,
            "year_month": month_clean,
            "metric": metric,
            "value": df[(month_raw, metric)]
        })
        monthly.append(temp)

    monthly_df = pd.concat(monthly, ignore_index=True)
    monthly_df = monthly_df[monthly_df["metric"] == "총인구수 (명)"] # 총인구수 데이터만 필터링
    monthly_df["value"] = pd.to_numeric(monthly_df["value"], errors="coerce")
    monthly_df["region_id"] = monthly_df["region_name"].map(REGION_CODE_MAP)
    monthly_df = monthly_df.dropna(subset=["region_id", "value"])
    monthly_df["region_id"] = monthly_df["region_id"].astype(int)

    # 최종 월별 Fact 테이블 생성
    fact_monthly = monthly_df[["region_id", "year_month", "value"]].copy()
    fact_monthly.rename(columns={"value": "total_pop"}, inplace=True)
    fact_monthly["total_pop"] = fact_monthly["total_pop"].astype(int)

    logger.info(f"✓ 인구 데이터 정제 완료: {len(fact_monthly)}행")

    return fact_monthly


def create_dimension_region() -> pd.DataFrame:
    """
    지역 차원 테이블(Dimension Table)을 생성함.
    - Dimension Table: Fact 테이블에 있는 코드(예: 11)가 실제 어떤 의미(예: "서울특별시")인지
      설명해주는 테이블. 데이터의 중복을 줄이고 일관성을 유지하게 도와줌.

    [출력] (region_id, region_name) 형태의 데이터프레임
    """

    regions = pd.DataFrame([
        {"region_id": code, "region_name": name}
        for name, code in REGION_CODE_MAP.items()
    ])

    # region_id 중복 제거: 같은 ID가 여러 지역명에 매핑된 경우, 최신 지역명 사용
    # (예: 42 -> "강원도", "강원특별자치도" 중 "강원특별자치도" 선택)
    regions = regions.drop_duplicates(subset=["region_id"], keep="last")

    return regions


def create_dimension_education() -> pd.DataFrame:
    """
    교육수준 차원 테이블을 생성함 (신규 데이터).

    [출력] (education_id, education_name)
    - ML 모델에서 '대졸자 취업률' 같은 파생 변수를 만들 때 사용됨.
    """

    education_levels = pd.DataFrame([
        {"education_id": 1, "education_name": "초졸이하"},
        {"education_id": 2, "education_name": "중졸"},
        {"education_id": 3, "education_name": "고졸"},
        {"education_id": 4, "education_name": "대졸이상"},
    ])

    logger.info(f"✓ 교육수준 차원 테이블 생성: {len(education_levels)}개")
    return education_levels


def create_dimension_age_group() -> pd.DataFrame:
    """
    연령대 차원 테이블을 생성함 (신규 데이터).

    [출력] (age_group_id, age_group_name)
    - ML 모델에서 연령대별 고용 패턴 분석에 사용됨.
    - 데이터 품질 개선: 중복 집계 방지를 위해 개별 연령대(1-6)만 포함.
    """

    age_groups = pd.DataFrame([
        {"age_group_id": 1, "age_group_name": "15-19세"},
        {"age_group_id": 2, "age_group_name": "20-29세"},
        {"age_group_id": 3, "age_group_name": "30-39세"},
        {"age_group_id": 4, "age_group_name": "40-49세"},
        {"age_group_id": 5, "age_group_name": "50-59세"},
        {"age_group_id": 6, "age_group_name": "60세이상"},
        # 집계 연령대는 제외 (중복 집계 방지)
        # {"age_group_id": 10, "age_group_name": "15-24세"},
        # {"age_group_id": 11, "age_group_name": "15-29세"},
        # {"age_group_id": 12, "age_group_name": "15-64세"},
    ])

    logger.info(f"✓ 연령대 차원 테이블 생성: {len(age_groups)}개")
    return age_groups


def extract_employment_insurance() -> pd.DataFrame:
    """
    고용보험 피보험자 데이터를 추출하고 정제함 (신규 데이터).

    [입력 파일] data/new_data/고용보험_월별_피보험자현황.csv

    [출력 컬럼]
    - region_id: 지역 코드
    - year_month: 연월
    - insured_count: 피보험자 수 (해당 월에 고용보험에 가입된 총 인원)
    - new_insured: 신규 취득자 수 (그 달에 새로 가입한 인원)
    - terminated_insured: 상실자 수 (그 달에 자격을 잃은 인원, 예: 퇴사)

    [ML 모델 활용 방안]
    - 고용의 질 지표: 고용보험가입률 (insured_count / 전체 취업자 수)
    - 노동시장 유동성 지표: 이직률 ((new_insured + terminated_insured) / insured_count)
    """

    csv_path = NEW_DATA_DIR / "고용보험_월별_피보험자현황.csv"

    if not csv_path.exists():
        logger.warning(f"파일 없음: {csv_path}. 빈 데이터프레임을 반환함.")
        return pd.DataFrame()

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info(f"✓ 고용보험 원본 로드: {df.shape}")

    # 이 CSV 파일은 구조가 매우 특이함.
    # 컬럼 구조: | 시도 | 2023년01월 | (취득) | (상실) | 2023년02월 | (취득) | (상실) | ...
    # 즉, 각 연월마다 3개 컬럼(피보험자수, 취득자, 상실자)이 반복되는 형태임.
    # 따라서 일반적인 melt 방식 대신, for loop를 돌면서 데이터를 직접 파싱해야 함.

    df_data = df.iloc[1:].copy()  # 2행부터가 실제 데이터
    region_col = df.columns[0]  # 첫 번째 컬럼이 지역명

    # '년'과 '월'이 들어간 컬럼(연월 컬럼)만 추출. 예: "2023년01월", "2023년02월" ...
    month_cols = [c for c in df.columns if '년' in str(c) and '월' in str(c)]

    # Long 형태로 변환하는 과정
    records = []
    for idx, row in df_data.iterrows():
        region_name = str(row[region_col]).strip()

        # 각 연월별로 순회
        for month_col in month_cols:
            # "2023년01월" → "2023-01" 형식으로 변환
            year_month = str(month_col).replace("년", "-").replace("월", "")

            # 해당 연월 컬럼의 위치(인덱스)를 찾음
            col_idx = df.columns.get_loc(month_col)

            # 연월 컬럼(피보험자) 바로 다음 2개 컬럼이 각각 취득자, 상실자임.
            if col_idx + 2 < len(df.columns):
                insured = row.iloc[col_idx]
                new_ins = row.iloc[col_idx + 1]
                term_ins = row.iloc[col_idx + 2]

                records.append({
                    "region_name": region_name,
                    "year_month": year_month,
                    "insured_count": insured,
                    "new_insured": new_ins,
                    "terminated_insured": term_ins
                })

    result = pd.DataFrame(records)

    # 값 정리: 숫자에 포함된 쉼표(,)를 제거하고 숫자 타입으로 변환.
    for col in ["insured_count", "new_insured", "terminated_insured"]:
        result[col] = pd.to_numeric(
            result[col].astype(str).str.replace(",", ""), errors="coerce"
        )

    # 지역명 정규화 및 불필요한 행('총계', '전국') 제거
    result["region_name"] = result["region_name"].replace({
        "제주도": "제주특별자치도",
        "총계": None,
        "전국": None
    })

    # 지역 코드로 변환
    result["region_id"] = result["region_name"].map(REGION_CODE_MAP)
    result = result.dropna(subset=["region_id", "insured_count"])
    result["region_id"] = result["region_id"].astype(int)
    result["insured_count"] = result["insured_count"].astype(int)

    # 결측치(NaN) 처리: 취득/상실자는 0명일 수 있으므로 0으로 채움.
    for col in ["new_insured", "terminated_insured"]:
        result[col] = result[col].fillna(0).astype(int)

    logger.info(f"✓ 고용보험 데이터 정제 완료: {len(result)}행, {result['region_id'].nunique()}개 지역")

    return result[["region_id", "year_month", "insured_count", "new_insured", "terminated_insured"]]


def extract_employment_by_education() -> pd.DataFrame:
    """
    교육수준별 취업자 데이터를 추출하고 정제함 (신규 데이터).

    [입력 파일] data/new_data/행정구역_시도__교육정도별_취업자_....csv

    [출력] (region_id, education_id, year_month, employed_count)

    [ML 모델 활용 방안]
    - education_id=4 (대졸이상) 데이터를 사용해서 '대졸자 취업자 비율' 등을 계산,
      고학력자 고용 시장 분석을 위한 피처로 활용함.
    """

    csv_path = NEW_DATA_DIR / "행정구역_시도__교육정도별_취업자_20251117204725.csv"

    if not csv_path.exists():
        logger.warning(f"파일 없음: {csv_path}. 빈 데이터프레임을 반환함.")
        return pd.DataFrame()

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info(f"✓ 교육수준별 취업자 원본 로드: {df.shape}")

    # 컬럼 구성: 시도별, 교육정도별, 2017.01, 2017.02, ...
    id_cols = ["시도별", "교육정도별"]
    value_cols = [c for c in df.columns if c not in id_cols]

    # Wide → Long 변환
    tidy = df.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="year_month",
        value_name="employed_count"
    )

    # 날짜 형식 통일
    tidy["year_month"] = tidy["year_month"].str.replace(".", "-")

    # 지역명 정규화 및 불필요 행 제거
    tidy["region_name"] = tidy["시도별"].str.strip().replace({
        "제주도": "제주특별자치도", "계": None, "전국": None
    })

    # 교육수준명을 교육 ID로 매핑
    education_map = {"초졸이하": 1, "중졸": 2, "고졸": 3, "대졸이상": 4}
    tidy["education_id"] = tidy["교육정도별"].str.strip().map(education_map)

    # 값 정리 (천명 단위 → 명 단위)
    tidy["employed_count"] = pd.to_numeric(tidy["employed_count"], errors="coerce") * 1_000

    # 결측치가 있는 행 제거
    tidy = tidy.dropna(subset=["region_name", "education_id", "employed_count"])

    # 지역 코드로 매핑 및 타입 변환
    tidy["region_id"] = tidy["region_name"].map(REGION_CODE_MAP)
    tidy = tidy.dropna(subset=["region_id"])
    tidy["region_id"] = tidy["region_id"].astype(int)
    tidy["education_id"] = tidy["education_id"].astype(int)
    tidy["employed_count"] = tidy["employed_count"].astype(int)

    logger.info(f"✓ 교육수준별 취업자 정제 완료: {len(tidy)}행")

    return tidy[["region_id", "education_id", "year_month", "employed_count"]]


def extract_employment_by_age() -> pd.DataFrame:
    """
    연령대별 취업자 데이터를 추출하고 정제함 (신규 데이터).

    [입력 파일] data/new_data/행정구역_시도__연령별_취업자_....csv

    [출력] (region_id, age_group_id, year_month, employed_count)

    [ML 모델 활용 방안]
    - age_group_id=11 (15-29세) 데이터를 사용해서 '청년 취업자 비율' 등을 계산,
      청년 고용 문제 분석을 위한 핵심 피처로 활용함.
    """

    csv_path = NEW_DATA_DIR / "행정구역_시도__연령별_취업자_20251117204639.csv"

    if not csv_path.exists():
        logger.warning(f"파일 없음: {csv_path}. 빈 데이터프레임을 반환함.")
        return pd.DataFrame()

    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    logger.info(f"✓ 연령대별 취업자 원본 로드: {df.shape}")

    # 컬럼 구성: 시도별(1), 연령계층별(1), 2017.01, 2017.02, ...
    id_cols = ["시도별(1)", "연령계층별(1)"]
    value_cols = [c for c in df.columns if c not in id_cols]

    # Wide → Long 변환
    tidy = df.melt(
        id_vars=id_cols,
        value_vars=value_cols,
        var_name="year_month",
        value_name="employed_count"
    )

    # 날짜 형식 통일
    tidy["year_month"] = tidy["year_month"].str.replace(".", "-")

    # 지역명 정규화
    tidy["region_name"] = tidy["시도별(1)"].str.strip().replace({
        "제주도": "제주특별자치도", "계": None, "전국": None
    })

    # ★ 데이터 품질 개선: 개별 연령대(1-6)만 사용하여 중복 집계 방지
    # 통계청 원천 데이터는 개별 연령대(15-19, 20-29...)와 집계 연령대(15-29, 15-64...)가 혼재되어 있어,
    # 단순 합계 시 동일 취업자가 2배 이상 집계되는 문제가 있음.
    # 따라서 상호 배타적인(mutually exclusive) 개별 연령대 6개 구간만 사용함.
    age_group_map = {
        "15 - 19세": 1, "20 - 29세": 2, "30 - 39세": 3, "40 - 49세": 4,
        "50 - 59세": 5, "60세이상": 6,
        # 집계 연령대는 매핑하지 않음 (중복 방지)
        # "15 - 24세": 10, "15 - 29세": 11, "15 - 64세": 12,
    }
    tidy["age_group_id"] = tidy["연령계층별(1)"].str.strip().map(age_group_map)

    # 값 정리 (천명 단위 → 명 단위)
    tidy["employed_count"] = pd.to_numeric(tidy["employed_count"], errors="coerce") * 1_000

    # 결측치 제거 (매핑되지 않은 연령대 등)
    tidy = tidy.dropna(subset=["region_name", "age_group_id", "employed_count"])

    # 지역 코드 매핑 및 타입 변환
    tidy["region_id"] = tidy["region_name"].map(REGION_CODE_MAP)
    tidy = tidy.dropna(subset=["region_id"])
    tidy["region_id"] = tidy["region_id"].astype(int)
    tidy["age_group_id"] = tidy["age_group_id"].astype(int)
    tidy["employed_count"] = tidy["employed_count"].astype(int)

    logger.info(f"✓ 연령대별 취업자 정제 완료: {len(tidy)}행")

    return tidy[["region_id", "age_group_id", "year_month", "employed_count"]]


# 이 파일이 직접 실행될 때 (예: python src/etl.py) 아래 코드를 실행함.
# 각 함수가 잘 작동하는지 개별적으로 테스트하기 위한 용도임.
if __name__ == "__main__":
    # 각 함수를 호출해서 결과를 변수에 저장
    unemployment = extract_unemployment()
    employment, industry = extract_employment()
    pop_monthly = extract_population()
    region = create_dimension_region()

    # 신규 데이터 함수들도 호출
    education_dim = create_dimension_education()
    age_dim = create_dimension_age_group()
    insurance = extract_employment_insurance()
    emp_by_edu = extract_employment_by_education()
    emp_by_age = extract_employment_by_age()

    # 각 함수 실행 후 반환된 데이터프레임의 모양(shape)을 출력해서 확인
    print("\n=== ETL 테스트 실행 결과 ===")
    print(f"실업률: {unemployment.shape}")
    print(f"고용: {employment.shape}")
    print(f"산업: {industry.shape}")
    print(f"인구(월별): {pop_monthly.shape}")
    print(f"지역: {region.shape}")
    print(f"\n[신규 데이터]")
    print(f"교육수준 차원: {education_dim.shape}")
    print(f"연령대 차원: {age_dim.shape}")
    print(f"고용보험: {insurance.shape}")
    print(f"교육수준별 취업자: {emp_by_edu.shape}")
    print(f"연령대별 취업자: {emp_by_age.shape}")
