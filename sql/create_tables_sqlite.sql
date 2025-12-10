-- =================================================================
-- 한국 시도별 노동시장 분석용 데이터베이스 스키마 (SQLite 버전)
-- =================================================================
-- 이 파일은 데이터베이스에 어떤 테이블들을 어떤 구조로 만들지 정의하는 설계도임.
-- '스타 스키마(Star Schema)' 구조를 따름.
--   - Dimension(차원) 테이블: '무엇'에 대한 정보 (지역, 산업, 연령대 등). 코드와 이름처럼 잘 변하지 않는 마스터 데이터.
--   - Fact(사실) 테이블: '어떤 일이 일어났는지'에 대한 정보 (월별 실업률, 취업자 수 등). 계속 쌓이는 측정 데이터.

-- 'CREATE TABLE IF NOT EXISTS' 구문: 만약 테이블이 존재하지 않을 경우에만 새로 생성함.

-- -----------------------------------------------------------------
-- Dimension(차원) 테이블들
-- -----------------------------------------------------------------

-- 지역 차원 테이블: 지역 코드와 지역 이름을 관리함.
CREATE TABLE IF NOT EXISTS dim_region (
    region_id      INTEGER PRIMARY KEY, -- 지역 코드 (예: 11). 이 테이블의 각 행을 구분하는 고유한 식별자(기본 키).
    region_name    TEXT NOT NULL UNIQUE -- 지역 이름 (예: '서울특별시'). 비어있을 수 없고(NOT NULL), 중복될 수 없음(UNIQUE).
);

-- 산업 차원 테이블: 산업 코드와 산업 이름을 관리함.
CREATE TABLE IF NOT EXISTS dim_industry (
    industry_code  TEXT PRIMARY KEY, -- 산업 코드 (예: 'A', 'C'). 기본 키.
    industry_name  TEXT NOT NULL     -- 산업 이름 (예: '농업, 임업 및 어업').
);

-- 교육수준 차원 테이블 (신규): 교육 수준 코드와 이름을 관리함.
CREATE TABLE IF NOT EXISTS dim_education (
    education_id   INTEGER PRIMARY KEY, -- 교육수준 코드 (예: 1, 2, 3, 4). 기본 키.
    education_name TEXT NOT NULL UNIQUE -- 교육수준 이름 (예: '대졸이상').
);

-- 연령대 차원 테이블 (신규): 연령대 코드와 이름을 관리함.
CREATE TABLE IF NOT EXISTS dim_age_group (
    age_group_id   INTEGER PRIMARY KEY, -- 연령대 코드 (예: 11 for '15-29세'). 기본 키.
    age_group_name TEXT NOT NULL UNIQUE -- 연령대 이름 (예: '15-29세').
);


-- -----------------------------------------------------------------
-- Fact(사실) 테이블들
-- -----------------------------------------------------------------
-- Fact 테이블들은 여러 Dimension 테이블의 기본 키를 '외래 키(Foreign Key)'로 가짐.
-- 이를 통해 여러 차원에서 데이터를 분석할 수 있게 됨. (예: '서울' 지역의 '제조업' 분야 '월별' 취업자 수)

-- 월별 실업률 팩트 테이블: 지역별 월별 실업 관련 지표들을 저장함.
CREATE TABLE IF NOT EXISTS fact_unemployment_monthly (
    region_id          INTEGER NOT NULL, -- 지역 코드 (dim_region 테이블을 참조하는 외래 키)
    year_month         TEXT NOT NULL,    -- 연월 (예: '2023-01')
    unemployment_rate  REAL NOT NULL,    -- 실업률 (%). REAL은 소수점을 포함하는 숫자 타입.
    unemployment_level INTEGER,          -- 실업자 수 (명). INTEGER는 정수 타입.
    labor_force        INTEGER,          -- 경제활동인구 (명)
    employed_persons   INTEGER,          -- 취업자 수 (명)

    -- PRIMARY KEY (region_id, year_month):
    -- 이 테이블에서는 '지역 코드'와 '연월' 두 개를 조합해야 각 행이 고유하게 식별됨 (복합 기본 키).
    PRIMARY KEY (region_id, year_month),

    -- FOREIGN KEY (region_id) REFERENCES dim_region (region_id):
    -- 이 테이블의 region_id는 반드시 dim_region 테이블에 존재하는 region_id 값이어야 한다는 제약조건.
    -- 데이터의 일관성과 무결성을 보장함.
    FOREIGN KEY (region_id) REFERENCES dim_region (region_id)
        ON UPDATE CASCADE  -- dim_region의 region_id가 바뀌면, 여기도 자동으로 따라 바뀜.
        ON DELETE RESTRICT -- dim_region의 지역이 삭제되려 할 때, 이 테이블에서 사용 중이면 삭제를 막음.
);

-- 산업별 월별 고용 팩트 테이블: 지역/산업/월별 취업자 수를 저장함.
CREATE TABLE IF NOT EXISTS fact_employment_by_industry_monthly (
    region_id        INTEGER NOT NULL, -- 지역 코드 (외래 키)
    industry_code    TEXT NOT NULL,    -- 산업 코드 (외래 키)
    year_month       TEXT NOT NULL,    -- 연월
    employed_persons INTEGER NOT NULL, -- 취업자 수 (명)

    -- 이 테이블은 지역, 산업, 연월 3개를 조합해야 행이 고유해짐.
    PRIMARY KEY (region_id, industry_code, year_month),
    FOREIGN KEY (region_id) REFERENCES dim_region (region_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (industry_code) REFERENCES dim_industry (industry_code) ON UPDATE CASCADE ON DELETE RESTRICT
);

-- 월별 인구 팩트 테이블: 지역별 월별 총인구 수를 저장함.
CREATE TABLE IF NOT EXISTS fact_population_monthly (
    region_id   INTEGER NOT NULL, -- 지역 코드 (외래 키)
    year_month  TEXT NOT NULL,    -- 연월
    total_pop   INTEGER NOT NULL, -- 총인구 수 (명)
    PRIMARY KEY (region_id, year_month),
    FOREIGN KEY (region_id) REFERENCES dim_region (region_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

-- 고용보험 피보험자 팩트 테이블 (신규): 지역별 월별 고용보험 현황을 저장함.
CREATE TABLE IF NOT EXISTS fact_employment_insurance (
    region_id         INTEGER NOT NULL, -- 지역 코드 (외래 키)
    year_month        TEXT NOT NULL,    -- 연월
    insured_count     INTEGER NOT NULL, -- 피보험자 수 (고용보험 가입자 수)
    new_insured       INTEGER,          -- 신규 취득자 수
    terminated_insured INTEGER,          -- 상실자 수 (퇴사, 해고 등)
    PRIMARY KEY (region_id, year_month),
    FOREIGN KEY (region_id) REFERENCES dim_region (region_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

-- 교육수준별 취업자 팩트 테이블 (신규): 지역/교육수준/월별 취업자 수를 저장함.
CREATE TABLE IF NOT EXISTS fact_employment_by_education (
    region_id      INTEGER NOT NULL, -- 지역 코드 (외래 키)
    education_id   INTEGER NOT NULL, -- 교육수준 코드 (외래 키)
    year_month     TEXT NOT NULL,    -- 연월
    employed_count INTEGER NOT NULL, -- 취업자 수 (명)
    PRIMARY KEY (region_id, education_id, year_month),
    FOREIGN KEY (region_id) REFERENCES dim_region (region_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (education_id) REFERENCES dim_education (education_id) ON UPDATE CASCADE ON DELETE RESTRICT
);

-- 연령대별 취업자 팩트 테이블 (신규): 지역/연령대/월별 취업자 수를 저장함.
CREATE TABLE IF NOT EXISTS fact_employment_by_age (
    region_id      INTEGER NOT NULL, -- 지역 코드 (외래 키)
    age_group_id   INTEGER NOT NULL, -- 연령대 코드 (외래 키)
    year_month     TEXT NOT NULL,    -- 연월
    employed_count INTEGER NOT NULL, -- 취업자 수 (명)
    PRIMARY KEY (region_id, age_group_id, year_month),
    FOREIGN KEY (region_id) REFERENCES dim_region (region_id) ON UPDATE CASCADE ON DELETE RESTRICT,
    FOREIGN KEY (age_group_id) REFERENCES dim_age_group (age_group_id) ON UPDATE CASCADE ON DELETE RESTRICT
);
