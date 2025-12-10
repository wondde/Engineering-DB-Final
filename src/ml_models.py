# -*- coding: utf-8 -*-
"""
머신러닝 모델 분석 모듈

[역할]
- DB에서 데이터를 읽어와 머신러닝(ML) 모델을 학습하고 분석 결과를 생성함.
- 주요 분석 내용은 아래와 같음.
  1. 실업률 예측: 과거 데이터를 기반으로 미래의 실업률을 예측함. (지도학습 - 회귀)
  2. 지역 클러스터링: 고용 특성이 비슷한 지역끼리 그룹으로 묶음. (비지도학습 - 군집화)
  3. 시계열 트렌드 분석: 시간에 따른 데이터 변화의 패턴을 분석함.

[사용 피처(Feature)]
- Feature: 모델이 예측을 위해 사용하는 입력 변수.
- 기존 피처 6개 + 신규 피처 4개 = 총 10개의 피처를 사용함.
  - 기존: 총인구, 경제활동참가율, 고용률, 연도, 월, 지역ID
  - 신규: 고용보험가입률, 청년고용비율, 대졸취업자비율, 이직률

[출력]
- 'output/ml_results/' 폴더에 분석 결과 그래프(PNG 파일)를 저장함.
"""

# --- 기본 라이브러리 임포트 ---
import logging
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor # 실업률 예측 모델
from sklearn.model_selection import train_test_split, cross_val_score # 데이터 분할 및 교차검증
from sklearn.preprocessing import StandardScaler # 데이터 스케일링 (클러스터링용)
from sklearn.cluster import KMeans # 지역 클러스터링 모델
from sklearn.metrics import mean_squared_error, r2_score, silhouette_score # 모델 성능 평가 지표
from sqlalchemy.engine import Engine
from sqlalchemy import text

# --- 로깅 및 경로/폰트 설정 ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "output" / "ml_results"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Matplotlib 그래프에서 한글이 깨지지 않도록 폰트 설정
import platform
import warnings
warnings.filterwarnings('ignore', category=UserWarning, module='matplotlib')

def setup_korean_font():
    """운영체제별로 한글 폰트를 자동 설정"""
    system = platform.system()

    if system == "Darwin":  # macOS
        plt.rcParams["font.family"] = "AppleGothic"
    elif system == "Windows":  # Windows
        # Windows에서 사용 가능한 폰트 시도
        try:
            plt.rcParams["font.family"] = "Malgun Gothic"
        except:
            plt.rcParams["font.family"] = "sans-serif"
    else:  # Linux
        # Linux에서는 NanumGothic이 설치되어 있지 않을 수 있으므로 DejaVu Sans 사용
        plt.rcParams["font.family"] = "sans-serif"

    plt.rcParams["axes.unicode_minus"] = False  # 마이너스 부호 깨짐 방지

# 폰트 설정 적용
setup_korean_font()


def load_ml_dataset(engine: Engine) -> pd.DataFrame:
    """
    머신러닝 학습에 필요한 모든 데이터를 DB에서 통합하여 불러오고,
    모델 학습에 사용할 피처(feature)들을 계산하여 데이터프레임으로 생성함.
    """
    # [SQL 쿼리 설명]
    # - WITH 구문 (CTE): 복잡한 쿼리를 논리적인 단계로 나누어 작성.
    #   - youth_employment: 연령대별 데이터에서 '청년(15-29세)' 취업자 수만 따로 계산.
    #   - education_stats: 교육수준별 데이터에서 '대졸이상' 취업자 수만 따로 계산.
    # - JOIN: 여러 테이블(실업률, 인구, 고용보험 등)을 'region_id'와 'year_month'를 기준으로 하나로 합침.
    # - 파생 변수(피처) 생성: 기존 데이터들을 조합하여 새로운 의미를 갖는 변수들을 만듦.
    #   예: insurance_coverage_rate (고용보험가입률) = 피보험자 수 / 전체 취업자 수
    query = text("""
    WITH youth_employment AS (
        SELECT region_id, year_month,
            SUM(CASE WHEN age_group_id = 11 THEN employed_count ELSE 0 END) as youth_employed,
            SUM(CASE WHEN age_group_id BETWEEN 1 AND 6 THEN employed_count ELSE 0 END) as total_employed_by_age
        FROM fact_employment_by_age GROUP BY region_id, year_month
    ),
    education_stats AS (
        SELECT region_id, year_month,
            SUM(CASE WHEN education_id = 4 THEN employed_count ELSE 0 END) as college_employed,
            SUM(employed_count) as total_employed_by_edu
        FROM fact_employment_by_education GROUP BY region_id, year_month
    )
    SELECT
        u.region_id, r.region_name, u.year_month, u.unemployment_rate,
        p.total_pop, i.insured_count, i.new_insured, i.terminated_insured,
        y.youth_employed, e.college_employed,
        /* --- 모델 학습에 사용될 파생 피처(Derived Feature)들 --- */
        CAST(u.labor_force AS FLOAT) / p.total_pop AS labor_force_ratio,
        CAST(u.employed_persons AS FLOAT) / p.total_pop AS employment_ratio,
        CAST(i.insured_count AS FLOAT) / u.employed_persons AS insurance_coverage_rate,
        CAST(y.youth_employed AS FLOAT) / y.total_employed_by_age AS youth_employment_rate,
        CAST(e.college_employed AS FLOAT) / e.total_employed_by_edu AS college_employment_rate,
        CAST((i.new_insured + i.terminated_insured) AS FLOAT) / i.insured_count AS turnover_rate,
        CAST(SUBSTR(u.year_month, 1, 4) AS INTEGER) AS year,
        CAST(SUBSTR(u.year_month, 6, 2) AS INTEGER) AS month
    FROM fact_unemployment_monthly u
    JOIN dim_region r ON u.region_id = r.region_id
    JOIN fact_population_monthly p ON u.region_id = p.region_id AND u.year_month = p.year_month
    LEFT JOIN fact_employment_insurance i ON u.region_id = i.region_id AND u.year_month = i.year_month
    LEFT JOIN youth_employment y ON u.region_id = y.region_id AND u.year_month = y.year_month
    LEFT JOIN education_stats e ON u.region_id = e.region_id AND u.year_month = e.year_month
    WHERE u.unemployment_rate IS NOT NULL AND p.total_pop > 0
    ORDER BY u.year_month, u.region_id
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    logger.info(f"✓ ML 데이터셋 로드 완료: {len(df)}행, {df.shape[1]}개 컬럼")
    return df


def train_unemployment_predictor(df: pd.DataFrame) -> Dict:
    """실업률 예측 모델을 학습하고 평가함 (Random Forest, Gradient Boosting)."""
    logger.info("=" * 80)
    logger.info("🤖 [AI 모델 1] 실업률 예측 모델 학습")
    logger.info("=" * 80)

    # 1. 피처(X)와 타겟(y) 변수 설정
    # - 타겟(y): 우리가 예측하려는 값. 여기서는 'unemployment_rate'(실업률).
    # - 피처(X): 예측에 사용할 입력 값들. 실업률과 직접적인 관계가 없는 독립적인 변수들만 사용.
    feature_cols = [
        "total_pop", "labor_force_ratio", "employment_ratio", "year", "month", "region_id",
        "insurance_coverage_rate", "youth_employment_rate", "college_employment_rate", "turnover_rate"
    ]
    X = df[feature_cols].copy()
    y = df["unemployment_rate"]

    # 2. 결측치(NaN) 처리
    # ML 모델은 빈 값을 처리하지 못하므로, 결측치가 포함된 행은 제거함.
    # 잘못된 값으로 채우는(imputation) 것보다, 확실한 데이터만 사용하는 것이 모델 성능에 더 좋을 수 있음.
    mask = X.notna().all(axis=1) & y.notna()
    X, y = X[mask], y[mask]
    logger.info(f"결측치 제거 후 학습 데이터: {len(X)}건")

    # 3. 학습 데이터와 테스트 데이터 분리 (시계열 데이터 방식)
    # [중요] 시계열 데이터는 절대 랜덤으로 섞으면 안 됨. 과거 데이터로 미래를 예측해야 하기 때문.
    # 여기서는 데이터의 앞 80%를 학습용으로, 뒤 20%를 테스트용으로 사용.
    # (예: 2017~2024년 데이터로 학습 -> 2025년 데이터로 예측 성능 테스트)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    logger.info(f"학습 데이터: {len(X_train)}건, 테스트 데이터: {len(X_test)}건")

    # 4. 모델 학습 및 평가
    # [모델 1: 랜덤 포레스트]
    # - 수백 개의 작은 의사결정나무(Decision Tree)를 만들고, 그 예측 결과를 종합(투표)하여 최종 예측값을 정하는 모델.
    # - 병렬 처리가 가능해 학습 속도가 빠르고, 일반적으로 성능이 좋음.
    rf_model = RandomForestRegressor(n_estimators=200, max_depth=15, random_state=42, n_jobs=-1)
    rf_model.fit(X_train, y_train)
    rf_pred = rf_model.predict(X_test)
    rf_r2 = r2_score(y_test, rf_pred) # R-squared: 모델의 설명력. 1에 가까울수록 좋음.
    rf_rmse = np.sqrt(mean_squared_error(y_test, rf_pred)) # RMSE: 예측 오차. 낮을수록 좋음.

    # [모델 2: 그래디언트 부스팅]
    # - 여러 개의 나무를 순차적으로 만들면서, 이전 나무의 예측 오차를 다음 나무가 보완해나가는 방식.
    # - 일반적으로 랜덤 포레스트보다 성능이 약간 더 좋지만, 학습 속도는 느림.
    gb_model = GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, random_state=42)
    gb_model.fit(X_train, y_train)
    gb_pred = gb_model.predict(X_test)
    gb_r2 = r2_score(y_test, gb_pred)
    gb_rmse = np.sqrt(mean_squared_error(y_test, gb_pred))

    # 5. 교차 검증 (Cross-Validation)
    # - 학습 데이터를 여러 개(cv=5)로 쪼개서, 모델을 여러 번 테스트하는 기법.
    # - 모델이 특정 데이터에만 과도하게 최적화(과적합)되는 것을 방지하고, 일반적인 성능을 측정할 수 있음.
    rf_cv_scores = cross_val_score(rf_model, X_train, y_train, cv=5, scoring="r2")
    gb_cv_scores = cross_val_score(gb_model, X_train, y_train, cv=5, scoring="r2")

    # 6. 결과 출력 및 시각화
    print("\n📊 모델 성능 비교")
    print(f"{'모델':<20} {'R² Score':<15} {'RMSE':<15} {'CV R² (평균)':<15}")
    print("-" * 65)
    print(f"{'Random Forest':<20} {rf_r2:<15.4f} {rf_rmse:<15.4f} {rf_cv_scores.mean():<15.4f}")
    print(f"{'Gradient Boosting':<20} {gb_r2:<15.4f} {gb_rmse:<15.4f} {gb_cv_scores.mean():<15.4f}")

    # 피처 중요도: 어떤 피처가 실업률 예측에 가장 큰 영향을 미쳤는지 보여줌.
    feature_importance = pd.DataFrame({
        "feature": feature_cols, "importance": rf_model.feature_importances_
    }).sort_values("importance", ascending=False)
    print("\n🔍 피처 중요도 (어떤 변수가 예측에 가장 중요한가?)")
    print(feature_importance.head(5).to_string(index=False))

    # 그래프 저장
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    axes[0].scatter(y_test, rf_pred, alpha=0.5, s=10)
    axes[0].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
    axes[0].set_title(f"Random Forest (R²={rf_r2:.4f})")
    axes[1].scatter(y_test, gb_pred, alpha=0.5, s=10, color='green')
    axes[1].plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
    axes[1].set_title(f"Gradient Boosting (R²={gb_r2:.4f})")
    plt.savefig(OUTPUT_DIR / "01_unemployment_prediction.png", dpi=300)
    plt.close()

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(feature_importance["feature"], feature_importance["importance"])
    ax.set_title("실업률 예측 피처 중요도")
    plt.savefig(OUTPUT_DIR / "02_feature_importance.png", dpi=300)
    plt.close()

    return {"feature_importance": feature_importance}


def run_region_clustering(df: pd.DataFrame) -> Dict:
    """지역 클러스터링 분석 (K-Means)을 통해 고용 특성이 비슷한 지역들을 그룹화함."""
    logger.info("\n" + "=" * 80)
    logger.info("🤖 [AI 모델 2] 지역 클러스터링 분석 (K-Means)")
    logger.info("=" * 80)

    # 1. 클러스터링용 데이터 생성
    # 각 지역별로 주요 지표들의 평균값을 계산함.
    region_stats = df.groupby("region_name").agg({
        "unemployment_rate": "mean",
        "employment_ratio": "mean",
        "insurance_coverage_rate": "mean",
        "youth_employment_rate": "mean"
    }).dropna()

    # 2. 데이터 스케일링 (StandardScaler)
    # - K-Means는 거리를 기반으로 그룹을 나누기 때문에, 각 피처의 단위(scale)가 다르면 왜곡이 발생함.
    #   (예: 인구수(백만 단위)와 실업률(%)을 그냥 비교하면 인구수의 영향이 훨씬 커짐)
    # - StandardScaler는 모든 피처를 평균 0, 표준편차 1인 정규분포로 변환하여 단위를 통일시켜 줌.
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(region_stats)

    # 3. 최적의 클러스터 개수(K) 찾기
    # - 실루엣 점수(Silhouette Score): -1~1 사이의 값. 1에 가까울수록 클러스터링이 잘 되었다는 의미.
    #   (같은 클러스터 내 데이터는 가깝고, 다른 클러스터 데이터와는 멀다는 뜻)
    silhouette_scores = []
    K_range = range(2, 10)
    for k in K_range:
        kmeans = KMeans(n_clusters=k, random_state=42, n_init='auto')
        kmeans.fit(X_scaled)
        silhouette_scores.append(silhouette_score(X_scaled, kmeans.labels_))
    
    optimal_k = K_range[np.argmax(silhouette_scores)]
    logger.info(f"최적 클러스터 개수(K)는 {optimal_k}로 결정됨 (실루엣 점수 최대).")

    # 4. K-Means 모델 학습 및 결과 분석
    kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init='auto')
    region_stats["cluster"] = kmeans.fit_predict(X_scaled)

    print(f"\n📊 클러스터링 결과 (K={optimal_k})")
    for i in range(optimal_k):
        print(f"\n[클러스터 {i}]")
        cluster_regions = region_stats[region_stats["cluster"] == i]
        print(f"  - 지역: {', '.join(cluster_regions.index)}")
        print(cluster_regions.describe().loc[["mean", "std"]].round(2).to_string())

    # 5. 시각화
    # PCA: 고차원(4개 피처) 데이터를 시각화를 위해 2차원으로 축소하는 기법.
    from sklearn.decomposition import PCA
    pca = PCA(n_components=2)
    X_pca = pca.fit_transform(X_scaled)
    
    plt.figure(figsize=(10, 8))
    sns.scatterplot(x=X_pca[:, 0], y=X_pca[:, 1], hue=region_stats['cluster'], palette='viridis', s=150, alpha=0.8)
    for i, region in enumerate(region_stats.index):
        plt.text(X_pca[i, 0]+0.05, X_pca[i, 1], region, fontsize=9)
    plt.title(f'지역 클러스터링 결과 (K={optimal_k})')
    plt.xlabel('PCA Component 1')
    plt.ylabel('PCA Component 2')
    plt.grid(True, alpha=0.3)
    plt.savefig(OUTPUT_DIR / "03_region_clustering.png", dpi=300)
    plt.close()

    return {"region_stats": region_stats}


def run_time_series_analysis(df: pd.DataFrame) -> Dict:
    """시계열 트렌드 분석을 통해 전체 실업률의 장기 추세와 계절성을 파악함."""
    logger.info("\n" + "=" * 80)
    logger.info("📈 [기술통계] 시계열 트렌드 분석")
    logger.info("=" * 80)

    # 1. 전국 월별 평균 실업률 계산
    ts_data = df.groupby("year_month")["unemployment_rate"].mean().reset_index()
    ts_data["year_month"] = pd.to_datetime(ts_data["year_month"])
    ts_data = ts_data.set_index("year_month")

    # 2. 시계열 분해 (Seasonal Decompose)
    # - 시계열 데이터를 추세(Trend), 계절성(Seasonality), 잔차(Residual) 세 가지 요소로 분해함.
    from statsmodels.tsa.seasonal import seasonal_decompose
    decomposition = seasonal_decompose(ts_data['unemployment_rate'], model='additive', period=12)

    # 3. 시각화
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 10), sharex=True)
    decomposition.observed.plot(ax=ax1, legend=False)
    ax1.set_ylabel('Observed')
    decomposition.trend.plot(ax=ax2, legend=False)
    ax2.set_ylabel('Trend')
    decomposition.seasonal.plot(ax=ax3, legend=False)
    ax3.set_ylabel('Seasonal')
    decomposition.resid.plot(ax=ax4, legend=False)
    ax4.set_ylabel('Residual')
    plt.suptitle('실업률 시계열 분해 (전국 평균)')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(OUTPUT_DIR / "04_time_series_trend.png", dpi=300)
    plt.close()

    print("시계열 분해 결과 '04_time_series_trend.png' 파일로 저장됨.")
    print("- Trend: 데이터의 장기적인 추세")
    print("- Seasonal: 특정 기간(12개월)마다 반복되는 패턴")
    print("- Residual: 추세와 계절성으로 설명되지 않는 나머지 변동(노이즈)")

    return {"decomposition": decomposition}


def run_all_ml_models(engine: Engine) -> Dict:
    """모든 머신러닝 모델 분석을 순서대로 실행하는 메인 함수."""
    logger.info("\n" + "=" * 80)
    logger.info("🚀 AI/ML 분석 파이프라인 시작")
    logger.info("=" * 80)

    # 1. ML 학습용 데이터셋 로드
    df = load_ml_dataset(engine)
    results = {}

    # 2. 실업률 예측 모델 실행
    results["prediction"] = train_unemployment_predictor(df)

    # 3. 지역 클러스터링 실행
    results["clustering"] = run_region_clustering(df)

    # 4. 시계열 트렌드 분석 실행
    results["time_series"] = run_time_series_analysis(df)

    logger.info("\n" + "=" * 80)
    logger.info("✅ AI/ML 분석 완료!")
    logger.info(f"📁 결과 그래프 저장 위치: {OUTPUT_DIR}")
    logger.info("=" * 80)

    return results


# 이 파일이 직접 실행될 때 (예: python src/ml_models.py) 아래 코드를 실행함.
# 이 모듈만 독립적으로 테스트하기 위한 용도임.
if __name__ == "__main__":
    from db_loader import DBConfig

    config = DBConfig()
    engine = config.make_engine()

    results = run_all_ml_models(engine)
