# -*- coding: utf-8 -*-
"""
노동시장 분석 프로젝트 - 메인 실행 파일

이 스크립트는 전체 데이터 분석 과정을 총괄하는 파일임.
이 파일 하나만 실행하면 데이터 추출, 정제, 데이터베이스 저장, 분석, 머신러닝 모델링까지
모든 작업이 순서대로 진행됨. 코드 보는 데에 참고하세용~

[사용법]
- 터미널에서 이 파일이 있는 폴더로 이동한 다음, 아래처럼 명령어를 입력함.

    python main.py --mode etl         # 1단계: 데이터 정제만 실행
    python main.py --mode load        # 2단계: 데이터베이스에 저장만 실행
    python main.py --mode analyze     # 3단계: SQL로 데이터 분석만 실행
    python main.py --mode ml          # 4단계: 인공지능 모델 분석만 실행
    python main.py --mode all         # 모든 단계를 순서대로 실행 (기본값)

[데이터베이스]
- SQLite라는 가벼운 파일 기반 데이터베이스를 사용함.
- 복잡한 설정이나 비밀번호 없이 'data/employment.db'라는 파일 하나에 모든 데이터가 저장됨.

[전체 작업 순서]
    1. ETL: 여러 CSV 파일에 흩어져 있는 데이터를 읽어서 분석하기 좋은 형태로 깨끗하게 만듦.
    2. Load: 정제된 데이터를 SQLite 데이터베이스에 차곡차곡 저장함.
    3. Analyze: SQL 쿼리를 사용해서 의미 있는 통계 정보를 뽑아내고 간단한 인사이트를 찾음.
    4. ML: 머신러닝 모델을 사용해서 미래의 실업률을 예측하거나, 지역별 고용 특성을 그룹으로 묶어보는 등 더 깊이 있는 분석을 함.
"""

# --- 기본 라이브러리 임포트 ---
import argparse  # 터미널 명령어의 옵션(argument)을 쉽게 다루게 해주는 라이브러리
import logging   # 프로그램 실행 중 발생하는 일에 대한 기록(로그)을 남길 때 사용함
import sys       # 파이썬 시스템 관련 기능을 다룰 때 필요. 여기서는 다른 폴더의 파일을 불러오기 위해 사용.
from pathlib import Path  # 파일/폴더 경로를 쉽게 다루게 해주는 라이브러리

# --- 직접 만든 다른 파이썬 파일(모듈) 임포트 ---

# 'src' 폴더 안에 직접 만든 중요 코드들이 있음.
# 파이썬이 해당 폴더를 찾을 수 있도록 경로를 알려주는 설정임.
sys.path.insert(0, str(Path(__file__).parent / "src"))

# 1. ETL(데이터 정제) 모듈: 'src/etl.py' 파일에서 필요한 함수들을 불러옴.
#    CSV 파일을 읽어서 판다스(Pandas) 데이터프레임(표 형태)으로 변환하는 함수들임.
from etl import (
    # --- 기존 데이터 (2017-2025) ---
    extract_unemployment,           # 실업률, 경제활동인구, 취업자, 실업자 데이터 추출
    extract_employment,             # 산업별 고용 현황 데이터 추출
    extract_population,             # 지역별 인구수 데이터 추출
    create_dimension_region,        # '서울', '부산' 같은 지역 이름과 코드를 정리한 표 생성

    # --- 신규 데이터 (2017-2025) ---
    # 지난 회의에서 얘기한대로, 인공지능 모델의 예측 정확도를 높이고, 더 다양한 분석을 위해 추가한 데이터임.
    create_dimension_education,     # '대졸', '고졸' 같은 교육 수준 정보를 정리한 표 생성
    create_dimension_age_group,     # '20대', '30대' 같은 연령대 정보를 정리한 표 생성
    extract_employment_insurance,   # 월별 고용보험 가입자 수 데이터 추출
    extract_employment_by_education,# 교육 수준별 취업자 수 데이터 추출
    extract_employment_by_age       # 연령대별 취업자 수 데이터 추출
)

# 2. DB 로더(데이터베이스 저장) 모듈: 'src/db_loader.py' 파일에서 필요한 함수들을 불러옴.
#    정제된 데이터프레임을 SQLite 데이터베이스에 저장하는 역할을 함.
from db_loader import DBConfig, load_to_database

# 3. 분석 모듈: 'src/analyzer.py' 파일에서 필요한 함수들을 불러옴.
#    DB에 저장된 데이터를 SQL 쿼리로 분석해서 의미있는 결과를 뽑아내는 역할을 함.
from analyzer import (
    run_all_insights,               # 실업률 트렌드, 산업별 고용 변화 등 기존 분석 실행
    print_insights,                 # 분석 결과를 출력
    run_basic_statistics,           # 데이터 개수, 기간 등 기본 통계 정보 확인
    run_new_insights,               # 고용보험, 청년 고용, 교육 수준 등 새로운 주제로 분석 실행
    print_new_insights              # 새로운 분석 결과를 출력
)

# 4. ML(머신러닝) 모듈: 'src/ml_models.py' 파일에서 필요한 함수를 불러옴.
#    인공지능 모델을 만들고 학습시켜 실업률 예측 등을 수행함.
from ml_models import run_all_ml_models

# --- 프로그램 실행 기록(로그)을 남기기 위한 기본 설정 ---
# 이 설정을 통해 프로그램 실행 중 "INFO: OOO 작업을 시작합니다." 같은 메시지를 시간과 함께 보여줌.
# 나중에 문제 발생 시 원인 파악을 용이하게 함.
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    """
    이 프로그램의 메인 함수.
    여기서부터 실질적인 모든 작업이 시작됨.
    """

    # --- 1단계: 사용자가 터미널에 입력한 명령어 해석 ---
    # "python main.py --mode etl" 처럼 입력된 명령어에서,
    # "--mode" 뒤의 "etl" 같은 값을 해석하는 부분임.
    parser = argparse.ArgumentParser(description="노동시장 데이터 분석 시스템 (SQLite + AI/ML)")
    parser.add_argument(
        "--mode",  # 옵션 이름
        choices=["etl", "load", "analyze", "ml", "all"],  # 정해진 값만 선택 가능
        default="all",  # 입력이 없으면 'all'을 기본값으로 사용
        help="실행할 작업 단계를 선택함."
    )
    parser.add_argument(
        "--db-path",  # DB 파일 경로를 지정하는 옵션
        default="data/employment.db",  # 기본 경로는 'data/employment.db'
        help="SQLite 데이터베이스 파일이 저장될 경로를 지정함."
    )

    args = parser.parse_args()  # 사용자가 입력한 옵션을 분석해 args 변수에 저장.

    # 데이터베이스 연결에 필요한 정보를 담는 설정 객체를 생성함.
    # SQLite는 파일 기반이므로 파일 경로만 지정해주면 됨.
    db_config = DBConfig(db_path=args.db_path)

    try:
        # 데이터 분석 시스템 시작
        print("\n" + "=" * 80)
        print("🚀 노동시장 데이터 분석 시스템")
        print("=" * 80 + "\n")

        # ========================================
        # STEP 1: ETL (데이터 추출, 변환, 적재의 첫 단계)
        # ========================================
        # CSV 파일들을 읽어서 분석하기 좋은 '데이터프레임' 형태로 만드는 과정.
        # 'etl' 또는 'all' 모드 선택 시 실행됨.
        if args.mode in ["etl", "all"]:
            logger.info("=" * 60)
            logger.info("STEP 1: ETL (데이터 정제 및 준비)")
            logger.info("=" * 60)

            # 'src/etl.py'의 함수들을 호출하여 데이터 로딩 및 정제.
            logger.info("[기존 데이터 정제 중...]")
            unemployment = extract_unemployment()      # 실업률 관련 데이터
            employment, industry = extract_employment() # 산업별 고용 데이터
            pop_monthly = extract_population()         # 월별 인구 데이터
            region = create_dimension_region()         # 지역 코드표

            # 머신러닝 모델 성능 향상을 위해 새로 추가한 데이터들도 정제.
            logger.info("\n[신규 데이터 정제 중...]")
            education = create_dimension_education()      # 교육수준 코드표
            age_group = create_dimension_age_group()      # 연령대 코드표
            insurance = extract_employment_insurance()    # 고용보험 가입 현황 데이터
            emp_by_edu = extract_employment_by_education()# 교육수준별 취업자 데이터
            emp_by_age = extract_employment_by_age()      # 연령대별 취업자 데이터

            logger.info("✅ 1단계 ETL 완료!\n")

        # ========================================
        # STEP 2: DB 적재 (데이터베이스에 저장)
        # ========================================
        # 정제된 데이터프레임들을 SQLite DB 파일에 '테이블'로 저장하는 과정.
        # 'load' 또는 'all' 모드 선택 시 실행됨.
        if args.mode in ["load", "all"]:
            logger.info("=" * 60)
            logger.info("STEP 2: DB 적재 (데이터를 SQLite에 저장)")
            logger.info("=" * 60)

            # 'load' 모드만 단독 실행 시, ETL 과정이 없었으므로 데이터가 없음.
            # 이 경우에만 특별히 ETL을 먼저 실행함.
            if args.mode == "load":
                logger.info("'load' 모드 단독 실행 감지. 먼저 ETL을 실행합니다.")
                unemployment = extract_unemployment()
                employment, industry = extract_employment()
                pop_monthly = extract_population()
                region = create_dimension_region()
                education = create_dimension_education()
                age_group = create_dimension_age_group()
                insurance = extract_employment_insurance()
                emp_by_edu = extract_employment_by_education()
                emp_by_age = extract_employment_by_age()

            # 데이터베이스 연결을 위한 '엔진' 생성.
            engine = db_config.make_engine()

            # 'src/db_loader.py'의 load_to_database 함수를 호출.
            # 모든 데이터프레임을 DB에 한 번에 저장함.
            # 만약 같은 이름의 테이블이 이미 있다면, 삭제 후 새로 생성 (if_exists='replace' 옵션).
            logger.info("데이터베이스에 테이블 쓰는 중...")
            load_to_database(
                engine,
                unemployment, employment, industry, pop_monthly, region,
                education=education,
                age_group=age_group,
                insurance=insurance,
                emp_by_edu=emp_by_edu,
                emp_by_age=emp_by_age
            )

            logger.info("✅ 2단계 DB 적재 완료!\n")

        # ========================================
        # STEP 3: SQL 분석 (데이터 분석)
        # ========================================
        # DB에 저장된 데이터를 SQL 쿼리로 분석하여 의미있는 정보를 추출하는 과정.
        # 'analyze' 또는 'all' 모드 선택 시 실행됨.
        if args.mode in ["analyze", "all"]:
            logger.info("=" * 60)
            logger.info("STEP 3: 데이터 분석 (SQL로 인사이트 찾기)")
            logger.info("=" * 60)

            # 분석을 위해 데이터베이스 연결 엔진을 다시 생성.
            engine = db_config.make_engine()

            # 'src/analyzer.py'의 함수들을 호출하여 분석 시작.
            logger.info("[기본 통계 정보 확인 중...]")
            run_basic_statistics(engine) # 전체 데이터 개수, 기간 등 기본 정보 출력

            logger.info("\n[주요 고용 지표 분석 중...]")
            insights = run_all_insights(engine) # 실업률, 고용 트렌드 등 주요 지표 분석
            print_insights(insights) # 분석 결과 출력

            logger.info("\n[신규 데이터 기반 심층 분석 중...]")
            new_insights = run_new_insights(engine) # 고용보험, 청년 고용 등 새로운 주제로 분석
            print_new_insights(new_insights) # 분석 결과 출력

            logger.info("✅ 3단계 SQL 분석 완료!\n")

        # ========================================
        # STEP 4: AI/ML 분석 (인공지능 모델링)
        # ========================================
        # 머신러닝 모델을 사용하여 복잡한 분석을 수행하는 과정.
        # 'ml' 또는 'all' 모드 선택 시 실행됨.
        if args.mode in ["ml", "all"]:
            logger.info("=" * 60)
            logger.info("STEP 4: AI/ML 분석 (미래 예측 및 클러스터링)")
            logger.info("=" * 60)

            # 모델링을 위해 데이터베이스 엔진을 생성.
            engine = db_config.make_engine()

            # 'src/ml_models.py'의 run_all_ml_models 함수를 호출하여 모델링 시작.
            # 이 함수 안에서 아래 3가지 모델이 순서대로 실행됨.
            # 1) 랜덤 포레스트 / 그라디언트 부스팅: 여러 지표를 바탕으로 미래 실업률을 '예측'하는 모델
            # 2) K-평균(K-Means): 고용 특성이 비슷한 지역끼리 '그룹'으로 묶어주는 모델 (클러스터링)
            # 3) 시계열 분석: 시간에 따른 데이터 변화 '트렌드'를 분석하는 모델
            logger.info("실업률 예측, 지역 클러스터링, 시계열 분석 모델 실행 중...")
            ml_results = run_all_ml_models(engine)

            logger.info("✅ 4단계 AI/ML 분석 완료! 결과는 'output/ml_results' 폴더에 저장됨.\n")

        # 모든 작업 완료.
        print("\n" + "=" * 80)
        print("✅ 모든 작업이 성공적으로 완료되었습니다!")
        print("=" * 80 + "\n")

    except Exception as e:
        # 작업 중 하나라도 오류 발생 시 이 부분이 실행됨.
        logger.error(f"❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()  # 오류 원인을 상세히 출력하여 디버깅을 도움.
        sys.exit(1)  # 오류 발생했으므로 프로그램 비정상 종료.


# 이 파이썬 파일을 'python main.py'처럼 직접 실행했을 때만 main() 함수를 호출하라는 의미.
# 다른 파일에서 'import main'처럼 이 파일을 불러와서 사용할 때는 main() 함수가 자동 실행되지 않음.
# 파이썬 코딩의 흔한 관례임.
if __name__ == "__main__":
    main()
