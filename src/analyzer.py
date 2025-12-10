# -*- coding: utf-8 -*-
"""
SQL 기반 분석기 (Analyzer)

[역할]
- SQLite DB에 연결하여 SQL 쿼리를 실행하고, 그 결과를 분석하여 통계 및 인사이트를 도출함.
- 'sql/' 폴더에 미리 작성된 .sql 파일을 읽어서 실행하는 '임베디드 SQL' 방식을 사용함.
  - 이렇게 하면 파이썬 코드와 SQL 코드가 분리되어 관리가 더 편해짐.

[분석 방법]
- 'sql/insights_sqlite.sql' 파일 안에 분석용 쿼리들을 미리 작성해둠.
- 각 쿼리는 '-- [쿼리이름]' 형식의 주석으로 구분함.
- 이 모듈에서는 그 쿼리 이름을 이용해 원하는 쿼리만 찾아서 실행함.
"""

# --- 기본 라이브러리 임포트 ---
import logging
import re
from pathlib import Path
from typing import Dict

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

# --- 로깅 및 경로 설정 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = PROJECT_ROOT / "sql"  # SQL 파일이 저장된 폴더


def execute_query_from_file(engine: Engine, sql_file: Path, query_name: str) -> pd.DataFrame:
    """
    SQL 파일에서 특정 이름의 쿼리만 찾아서 실행하고 결과를 데이터프레임으로 반환함.
    주석을 포함한 쿼리 블록 전체를 읽도록 수정됨.
    """
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    # 정규표현식을 사용하여 '-- [인사이트 N]' 형식으로 구분된 쿼리 블록들을 추출함.
    # re.DOTALL: '.'이 줄바꿈 문자도 포함하도록 함.
    # 패턴 설명: -- [인사이트 N] 뒤에 나오는 모든 내용을 다음 -- [인사이트까지 또는 파일 끝까지 가져옴
    pattern = r"--\s*\[인사이트\s+(\d+)\][^\n]*\n(.*?)(?=--\s*\[인사이트\s+\d+\]|$)"
    queries = {f"인사이트 {num}": query.strip() for num, query in re.findall(pattern, content, re.DOTALL)}

    if query_name not in queries:
        raise ValueError(f"SQL 파일 '{sql_file.name}'에서 '{query_name}' 쿼리를 찾을 수 없음.")

    query = queries[query_name]

    # DB에 연결하여 해당 SQL 쿼리를 실행하고, 결과를 판다스 데이터프레임으로 바로 읽어옴.
    with engine.connect() as conn:
        result = pd.read_sql_query(text(query), conn)

    logger.info(f"✓ SQL 실행 완료: {query_name} ({len(result)}행 반환)")
    return result


def run_all_insights(engine: Engine) -> Dict[str, pd.DataFrame]:
    """'insights_sqlite.sql' 파일에 정의된 모든 기존 인사이트 쿼리를 실행함."""
    insights = {}
    sql_file = SQL_DIR / "insights_sqlite.sql"

    # 실행할 쿼리 이름 목록 (1~8번)
    insight_names = [
        "인사이트 1", "인사이트 2", "인사이트 3", "인사이트 4",
        "인사이트 5", "인사이트 6", "인사이트 7", "인사이트 8"
    ]

    for name in insight_names:
        try:
            insights[name] = execute_query_from_file(engine, sql_file, name)
        except Exception as e:
            logger.error(f"✗ '{name}' 쿼리 실행 실패: {e}")
            insights[name] = pd.DataFrame() # 실패 시 빈 데이터프레임 반환

    return insights


def run_new_insights(engine: Engine) -> Dict[str, pd.DataFrame]:
    """'insights_sqlite.sql' 파일에 정의된 신규 인사이트 쿼리를 실행함."""
    insights = {}
    sql_file = SQL_DIR / "insights_sqlite.sql"

    # 실행할 쿼리 이름 목록 (9~15번)
    insight_names = [
        "인사이트 9", "인사이트 10", "인사이트 11", "인사이트 12",
        "인사이트 13", "인사이트 14", "인사이트 15"
    ]

    for name in insight_names:
        try:
            insights[name] = execute_query_from_file(engine, sql_file, name)
        except Exception as e:
            logger.error(f"✗ '{name}' 쿼리 실행 실패: {e}")
            insights[name] = pd.DataFrame() # 실패 시 빈 데이터프레임 반환

    return insights


def print_insights(insights: Dict[str, pd.DataFrame]) -> None:
    """분석 결과를 보기 좋게 출력함."""
    print("\n" + "=" * 80)
    print("📊 노동시장 데이터 분석 결과 (SQL 기반)")
    print("=" * 80 + "\n")

    for name, df in insights.items():
        print(f"[{name}]")
        print("-" * 80)
        if not df.empty:
            # 데이터가 너무 많을 수 있으므로 상위 10개만 출력
            print(df.head(10).to_string(index=False))
            if len(df) > 10:
                print(f"\n... (총 {len(df)}행 중 10행 표시)")
        else:
            print("데이터 없음 또는 분석 실패")
        print("\n")

    print("=" * 80 + "\n")


def print_new_insights(insights: Dict[str, pd.DataFrame]) -> None:
    """신규 인사이트 분석 결과를 보기 좋게 출력함."""
    print("\n" + "=" * 80)
    print("📊 신규 데이터 기반 심층 분석 결과")
    print("=" * 80 + "\n")

    for name, df in insights.items():
        print(f"[{name}]")
        print("-" * 80)
        if not df.empty:
            # 데이터가 너무 많을 수 있으므로 상위 10개만 출력
            print(df.head(10).to_string(index=False))
            if len(df) > 10:
                print(f"\n... (총 {len(df)}행 중 10행 표시)")
        else:
            print("데이터 없음 또는 분석 실패")
        print("\n")

    print("=" * 80 + "\n")


def run_basic_statistics(engine: Engine) -> None:
    """DB에 저장된 데이터의 기본적인 현황(행 개수 등)을 요약하여 보여줌."""
    print("\n" + "=" * 80)
    print("📈 기본 통계")
    print("=" * 80 + "\n")

    try:
        with engine.connect() as conn:
            stats = pd.read_sql_query(text("""
                SELECT
                    (SELECT COUNT(*) FROM fact_unemployment_monthly) as unemployment_rows,
                    (SELECT COUNT(*) FROM fact_employment_by_industry_monthly) as employment_rows,
                    (SELECT COUNT(*) FROM dim_industry) as industries,
                    (SELECT COUNT(*) FROM dim_region) as regions,
                    (SELECT COUNT(*) FROM fact_employment_insurance) as insurance_rows,
                    (SELECT COUNT(*) FROM fact_employment_by_education) as education_rows,
                    (SELECT COUNT(*) FROM fact_employment_by_age) as age_rows
            """), conn)

        print("데이터 현황:")
        print(f"  - 실업률 데이터: {stats['unemployment_rows'][0]:,}행")
        print(f"  - 산업별 고용 데이터: {stats['employment_rows'][0]:,}행")
        print(f"  - 산업 수: {stats['industries'][0]}개")
        print(f"  - 지역 수: {stats['regions'][0]}개")
        print(f"\n[신규 데이터]")
        print(f"  - 고용보험 데이터: {stats['insurance_rows'][0]:,}행")
        print(f"  - 교육수준별 취업자: {stats['education_rows'][0]:,}행")
        print(f"  - 연령대별 취업자: {stats['age_rows'][0]:,}행\n")
    except Exception as e:
        logger.error(f"✗ 기본 통계 조회 실패: {e}")


# run_new_insights와 print_new_insights는 run_all_insights와 print_insights로 통합되었으므로 제거.
# 각 analyze_* 함수들도 SQL 파일 호출 방식으로 변경되었으므로 제거.

# 이 파일이 직접 실행될 때 (예: python src/analyzer.py) 아래 코드를 실행함.
# 이 모듈만 독립적으로 테스트하기 위한 용도임.
if __name__ == "__main__":
    from db_loader import DBConfig

    # DB 연결
    config = DBConfig()
    engine = config.make_engine()

    # 기본 통계 실행 및 출력
    run_basic_statistics(engine)

    # 모든 인사이트 실행 및 출력
    insights = run_all_insights(engine)
    print_insights(insights)
