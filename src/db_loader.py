# -*- coding: utf-8 -*-
"""
DB Loader 모듈

[역할]
- ETL 과정에서 정제된 데이터프레임(DataFrame)을 SQLite 데이터베이스에 적재(Load)함.
- 별도의 DB 서버가 필요 없는 임베디드(내장형) SQL 방식을 사용함.

[주요 기능]
1. DB 연결 설정 (SQLite)
2. 테이블 생성 스크립트 실행 (CREATE TABLE)
3. 테이블의 기존 데이터 삭제 (DELETE)
4. 데이터프레임을 테이블에 적재 (INSERT)

[테이블 구조] - 데이터 웨어하우스의 '스타 스키마' 구조를 따름
- Dimension (차원) 테이블: '무엇'에 대한 정보. (예: 지역, 산업, 교육수준, 연령대 코드와 이름)
  - dim_region, dim_industry, dim_education, dim_age_group
- Fact (사실) 테이블: 실제 '측정값'에 대한 정보. (예: 월별 실업률, 취업자 수)
  - fact_unemployment_monthly, fact_employment_by_industry_monthly,
    fact_population_monthly, fact_employment_insurance,
    fact_employment_by_education, fact_employment_by_age
"""

# --- 기본 라이브러리 임포트 ---
from dataclasses import dataclass  # 간단한 데이터 클래스를 만들 때 사용
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text  # DB 연결 및 SQL 실행을 위한 라이브러리
from sqlalchemy.engine import Engine

# --- 로깅 및 경로 설정 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"      # CREATE TABLE 같은 SQL 스크립트가 있는 폴더
DB_DIR = PROJECT_ROOT / "data"      # DB 파일이 저장될 폴더


@dataclass
class DBConfig:
    """
    SQLite 데이터베이스 접속 정보를 담는 클래스.

    [SQLite의 특징]
    - 별도의 서버 설치나 실행이 필요 없는 파일 기반 데이터베이스임.
    - 아이디/비밀번호가 필요 없음.
    - db_path에 지정된 경로에 파일이 없으면 자동으로 생성됨. 매우 편리함.
    """
    db_path: str = "data/employment.db"  # DB 파일이 저장될 기본 경로

    def make_engine(self) -> Engine:
        """
        SQLAlchemy 라이브러리의 'Engine'을 생성함.
        - Engine: 데이터베이스와의 실제 연결을 관리하는 핵심 객체.
                  이 엔진을 통해 SQL 쿼리를 보내고 결과를 받을 수 있음.
        """
        # 프로젝트 루트를 기준으로 DB 파일의 전체(절대) 경로를 만듦.
        db_file = PROJECT_ROOT / self.db_path
        # DB 파일이 저장될 폴더가 없으면 자동으로 생성함.
        db_file.parent.mkdir(parents=True, exist_ok=True)

        # SQLAlchemy에서 사용하는 DB 연결 주소(URI) 형식임.
        # SQLite의 경우 "sqlite:///파일의/절대/경로" 형식을 사용함.
        uri = f"sqlite:///{db_file}"
        logger.info(f"SQLite DB 경로: {db_file}")
        # create_engine 함수로 엔진 생성. echo=True로 설정하면 실행되는 모든 SQL 쿼리가 화면에 출력됨 (디버깅용).
        return create_engine(uri, echo=False)


def execute_sql_file(engine: Engine, sql_file: Path) -> None:
    """
    지정된 SQL 파일을 읽어서 실행함. (주로 CREATE TABLE 스크립트를 실행할 때 사용)
    """
    if not sql_file.exists():
        logger.warning(f"SQL 파일 없음: {sql_file}. 실행을 건너뜀.")
        return

    # SQL 파일을 읽음.
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # SQL 파일 내용물을 세미콜론(;) 기준으로 쿼리 여러 개로 분리함.
    statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

    # DB에 연결해서 분리된 SQL 쿼리를 하나씩 실행.
    with engine.connect() as conn:
        for i, stmt in enumerate(statements, 1):
            try:
                conn.execute(text(stmt))
                conn.commit() # 변경사항을 DB에 최종 반영 (커밋)
                logger.info(f"✓ SQL 실행 완료: {sql_file.name} ({i}/{len(statements)})")
            except Exception as e:
                # "already exists" 에러는 테이블이 이미 존재한다는 뜻이므로, 무시하고 계속 진행.
                # 그 외의 다른 에러가 발생하면 로그를 남김.
                if "already exists" not in str(e).lower():
                    logger.error(f"✗ SQL 실행 실패: {e}")


def load_to_database(
    engine: Engine,
    unemployment: pd.DataFrame,
    employment: pd.DataFrame,
    industry: pd.DataFrame,
    pop_monthly: pd.DataFrame,
    region: pd.DataFrame,
    # --- 신규 파라미터 (기본값 None으로 지정하여 필수가 아님을 표시) ---
    education: pd.DataFrame = None,
    age_group: pd.DataFrame = None,
    insurance: pd.DataFrame = None,
    emp_by_edu: pd.DataFrame = None,
    emp_by_age: pd.DataFrame = None,
) -> None:
    """
    정제된 데이터프레임들을 SQLite 데이터베이스에 적재함.

    [처리 순서]
    1. 테이블 생성 (CREATE TABLE): DB에 테이블이 아직 없다면 생성함.
    2. 기존 데이터 삭제 (DELETE): 매번 실행할 때마다 데이터가 중복으로 쌓이는 것을 방지하기 위함.
    3. 데이터 삽입 (INSERT): Dimension 테이블을 먼저 넣고, 그 다음에 Fact 테이블을 넣음.

    [중요]
    - Dimension 테이블을 먼저 적재해야 함. Fact 테이블이 Dimension 테이블의 ID를 참조(외래 키, FK)하기 때문.
      순서가 바뀌면 "외래 키 제약조건 위반" 에러가 발생함.
    - SQLite는 TRUNCATE TABLE 명령어가 없어서 DELETE FROM을 사용함.
    """

    # ========================================
    # STEP 1: 테이블 생성 (없으면 생성)
    # ========================================
    logger.info("1. 테이블 생성 스크립트 실행 중...")
    # 'sql/create_tables_sqlite.sql' 파일을 읽어서 테이블 생성 쿼리들을 실행함.
    execute_sql_file(engine, SQL_DIR / "create_tables_sqlite.sql")

    # ========================================
    # STEP 2: 기존 데이터 삭제
    # ========================================
    # 이 과정을 거치지 않으면, 프로그램을 실행할 때마다 같은 데이터가 계속 추가됨.
    logger.info("2. 기존 데이터 삭제 중...")
    with engine.connect() as conn:
        # [핵심] 외래 키(Foreign Key) 제약조건을 잠시 끔.
        # 이유: 테이블 간의 참조 관계가 얽혀있어서, 데이터를 지우는 순서가 꼬이면 에러가 발생함.
        #      예를 들어, dim_region을 참조하는 fact 테이블이 있는데 dim_region을 먼저 지우려고 하면 에러 발생.
        #      따라서 제약조건을 잠시 끄고 안전하게 모든 데이터를 지운 뒤, 다시 켬.
        conn.execute(text("PRAGMA foreign_keys = OFF"))

        # 모든 테이블의 데이터를 삭제 (테이블 구조는 남겨둠)
        tables_to_delete = [
            "fact_unemployment_monthly", "fact_employment_by_industry_monthly",
            "fact_population_monthly", "dim_industry", "dim_region",
            "fact_employment_insurance", "fact_employment_by_education",
            "fact_employment_by_age", "dim_education", "dim_age_group"
        ]
        for table in tables_to_delete:
            conn.execute(text(f"DELETE FROM {table}"))

        # 외래 키 제약조건을 다시 켬 (데이터 무결성을 위해 중요).
        conn.execute(text("PRAGMA foreign_keys = ON"))
        conn.commit()  # 모든 삭제 작업을 최종 반영.
    logger.info("✓ 기존 데이터 삭제 완료")

    # ========================================
    # STEP 3: Dimension 테이블 적재
    # ========================================
    # Dimension(차원) 테이블: '지역', '산업' 등 Fact 데이터의 기준이 되는 마스터 정보.
    # 이 테이블들을 먼저 적재해야, 나중에 Fact 테이블이 이들을 참조할 수 있음.
    logger.info("3. Dimension(차원) 테이블 적재 중...")

    # to_sql(): 판다스 데이터프레임을 DB 테이블로 저장하는 매우 편리한 함수.
    # - if_exists="append": 테이블이 있으면 데이터를 추가함. (DELETE를 했으므로 사실상 새로 INSERT)
    # - index=False: 데이터프레임의 인덱스(0, 1, 2...)는 DB에 저장하지 않음.
    region.to_sql("dim_region", engine, if_exists="append", index=False)
    industry.to_sql("dim_industry", engine, if_exists="append", index=False)
    logger.info(f"✓ Dimension 적재: 지역 {len(region)}건, 산업 {len(industry)}건")

    # 신규 Dimension 테이블 적재 (데이터가 있을 경우에만)
    if education is not None and not education.empty:
        education.to_sql("dim_education", engine, if_exists="append", index=False)
        logger.info(f"✓ [신규] 교육수준 차원 적재: {len(education)}건")

    if age_group is not None and not age_group.empty:
        age_group.to_sql("dim_age_group", engine, if_exists="append", index=False)
        logger.info(f"✓ [신규] 연령대 차원 적재: {len(age_group)}건")

    # ========================================
    # STEP 4: Fact 테이블 적재
    # ========================================
    # Fact(사실) 테이블: 실제 측정값(실업률, 취업자 수 등)을 담고 있는 핵심 데이터.
    # 이 테이블들은 region_id, industry_code 등으로 Dimension 테이블을 참조함 (외래 키 관계).
    logger.info("4. Fact(사실) 테이블 적재 중...")

    # 기존 Fact 테이블 적재
    unemployment.to_sql("fact_unemployment_monthly", engine, if_exists="append", index=False)
    employment.to_sql("fact_employment_by_industry_monthly", engine, if_exists="append", index=False)
    pop_monthly.to_sql("fact_population_monthly", engine, if_exists="append", index=False)
    logger.info(f"✓ Fact 적재: 실업률 {len(unemployment)}건, 고용 {len(employment)}건, 인구 {len(pop_monthly)}건")

    # 신규 Fact 테이블 적재 (ML 모델의 피처(feature)로 활용될 데이터들)
    # 이 데이터들을 기반으로 '고용보험가입률', '청년고용률' 등 새로운 변수를 만들어 모델 성능을 높임.
    if insurance is not None and not insurance.empty:
        insurance.to_sql("fact_employment_insurance", engine, if_exists="append", index=False)
        logger.info(f"✓ [신규] 고용보험 적재: {len(insurance)}건")

    if emp_by_edu is not None and not emp_by_edu.empty:
        emp_by_edu.to_sql("fact_employment_by_education", engine, if_exists="append", index=False)
        logger.info(f"✓ [신규] 교육수준별 취업자 적재: {len(emp_by_edu)}건")

    if emp_by_age is not None and not emp_by_age.empty:
        emp_by_age.to_sql("fact_employment_by_age", engine, if_exists="append", index=False)
        logger.info(f"✓ [신규] 연령대별 취업자 적재: {len(emp_by_age)}건")

    logger.info("=" * 60)
    logger.info("✅ 모든 데이터의 DB 적재가 완료되었습니다!")
    logger.info("=" * 60)


# 이 파일이 직접 실행될 때 (예: python src/db_loader.py) 아래 코드를 실행함.
# 이 모듈만 독립적으로 테스트하기 위한 용도임.
if __name__ == "__main__":
    from etl import (
        extract_unemployment,
        extract_employment,
        extract_population,
        create_dimension_region,
        # 신규 데이터 함수들 추가
        create_dimension_education,
        create_dimension_age_group,
        extract_employment_insurance,
        extract_employment_by_education,
        extract_employment_by_age
    )

    # DB 연결 설정 및 엔진 생성
    config = DBConfig()
    engine = config.make_engine()

    # ETL 함수를 호출하여 데이터프레임을 가져옴
    logger.info("테스트 실행: ETL 시작...")
    # 기존 데이터
    unemployment = extract_unemployment()
    employment, industry = extract_employment()
    pop_monthly = extract_population()
    region = create_dimension_region()
    # 신규 데이터
    education = create_dimension_education()
    age_group = create_dimension_age_group()
    insurance = extract_employment_insurance()
    emp_by_edu = extract_employment_by_education()
    emp_by_age = extract_employment_by_age()
    logger.info("테스트 실행: ETL 완료.")

    # DB 적재 함수 호출
    logger.info("테스트 실행: DB 적재 시작...")
    load_to_database(
        engine, unemployment, employment, industry, pop_monthly, region,
        education=education, age_group=age_group, insurance=insurance,
        emp_by_edu=emp_by_edu, emp_by_age=emp_by_age
    )
    logger.info("테스트 실행: DB 적재 완료.")
