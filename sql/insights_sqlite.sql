-- =================================================================
-- 핵심 인사이트 도출을 위한 SQL 쿼리 모음 (SQLite 버전)
-- =================================================================
-- 이 파일의 쿼리들은 'analyzer.py'에서 호출되어 실제 데이터 분석을 수행함.
-- 각 쿼리는 '-- [이름]' 형식의 주석으로 구분됨.

-- -----------------------------------------------------------------
-- [인사이트 1] 어떤 산업이 일자리 증감에 가장 큰 영향을 미쳤는가?
-- -----------------------------------------------------------------
-- 최근(2020년 이후) 데이터에서, 전년 대비 취업자 수 변화가 가장 컸던 산업 Top 5를 찾음.
-- 이를 통해 어떤 산업이 고용 시장을 주도하고 있는지 파악할 수 있음.

-- 1. employment_growth: 지역/산업/연도별로 평균 취업자 수를 계산함.
WITH employment_growth AS (
    SELECT
        region_id,
        industry_code,
        CAST(SUBSTR(year_month, 1, 4) AS INTEGER) as year,
        AVG(employed_persons) as avg_employed
    FROM fact_employment_by_industry_monthly
    WHERE year_month >= '2020-01' -- 최근 데이터만 대상으로 함
    GROUP BY region_id, industry_code, CAST(SUBSTR(year_month, 1, 4) AS INTEGER)
),
-- 2. yoy_growth: 위에서 구한 연도별 평균 취업자 수를 바탕으로, 전년 대비 취업자 수 변화량(employment_change)을 계산함.
--    LEFT JOIN을 사용해 현재 연도(curr)와 이전 연도(prev) 테이블을 스스로(self) 조인하는 기술을 사용.
yoy_growth AS (
    SELECT
        curr.region_id,
        curr.industry_code,
        curr.year,
        curr.avg_employed - prev.avg_employed as employment_change
    FROM employment_growth curr
    LEFT JOIN employment_growth prev
        ON curr.region_id = prev.region_id
        AND curr.industry_code = prev.industry_code
        AND curr.year = prev.year + 1 -- 현재 연도 = 이전 연도 + 1
    WHERE prev.avg_employed IS NOT NULL -- 이전 연도 데이터가 있는 경우만
)
-- 3. 최종 결과: 산업별로 총 취업자 수 변화량을 합산(SUM)하고, 가장 많이 증가한 순서대로 정렬.
SELECT
    i.industry_name,
    COUNT(*) as observations, -- 몇 개의 관측치(지역/연도 조합)를 바탕으로 계산했는지
    ROUND(AVG(g.employment_change), 0) as avg_employment_change, -- 연평균 취업자 변화량
    ROUND(SUM(g.employment_change), 0) as total_employment_change -- 총 취업자 변화량
FROM yoy_growth g
JOIN dim_industry i ON g.industry_code = i.industry_code
GROUP BY i.industry_code, i.industry_name
HAVING COUNT(*) >= 10 -- 최소 10개 이상의 데이터가 있는 신뢰할 수 있는 결과만 필터링
ORDER BY total_employment_change DESC -- 총 변화량이 큰 순서로 정렬
LIMIT 5; -- 상위 5개만 표시

-- -----------------------------------------------------------------
-- [인사이트 2] 어느 지역의 실업률 변동성이 가장 큰가? (고용 불안정성 지표)
-- -----------------------------------------------------------------
-- 지역별 실업률의 표준편차(standard deviation)를 계산하여, 실업률이 안정적인지 혹은 불안정한지를 평가함.
-- 표준편차가 크면 실업률이 급등락을 반복했다는 의미로, 고용이 불안정하다고 해석할 수 있음.

-- 1. monthly_stats: 지역별로 실업률의 평균, 표준편차, 범위를 계산함.
--    [참고] SQLite에는 표준편차 함수(STDDEV)가 없어서, 'SQRT(AVG(x*x) - AVG(x)*AVG(x))' 공식을 이용해 직접 계산함.
WITH monthly_stats AS (
    SELECT
        region_id,
        AVG(unemployment_rate) as avg_rate,
        SQRT(AVG(unemployment_rate * unemployment_rate) - AVG(unemployment_rate) * AVG(unemployment_rate)) as std_rate,
        MAX(unemployment_rate) - MIN(unemployment_rate) as rate_range
    FROM fact_unemployment_monthly
    WHERE year_month >= '2020-01'
    GROUP BY region_id
)
-- 2. 최종 결과: 계산된 통계치와 함께 변동성 수준('높음'/'중간'/'안정')을 부여하고, 표준편차가 큰 순서로 정렬.
SELECT
    r.region_name,
    ROUND(s.avg_rate, 2) as avg_unemployment_rate,
    ROUND(s.std_rate, 2) as std_dev,
    ROUND(s.rate_range, 2) as rate_range,
    CASE
        WHEN s.std_rate > 0.5 THEN '높음'
        WHEN s.std_rate > 0.3 THEN '중간'
        ELSE '안정'
    END as volatility_level
FROM monthly_stats s
JOIN dim_region r ON s.region_id = r.region_id
ORDER BY s.std_rate DESC;

-- -----------------------------------------------------------------
-- [인사이트 3] 어느 지역의 산업 구조가 가장 다각화되어 있는가?
-- -----------------------------------------------------------------
-- 허핀달-허쉬만 지수(HHI)를 응용하여 '산업 다각화 지수'를 계산함.
-- 이 지수는 0~1 사이의 값을 가지며, 1에 가까울수록 여러 산업에 고용이 분산되어 있어 건강한 산업 구조임을 의미함.
-- 반대로 특정 산업에 고용이 쏠려있으면 지수가 낮아지고, 해당 산업이 위기에 처했을 때 지역 경제 전체가 위험해질 수 있음.

-- 1. region_industry_share: 지역별/산업별 평균 취업자 수를 계산.
WITH region_industry_share AS (
    SELECT e.region_id, e.industry_code, AVG(e.employed_persons) as avg_employed
    FROM fact_employment_by_industry_monthly e
    WHERE e.year_month >= '2023-01' -- 최근 2년 데이터 기준
    GROUP BY e.region_id, e.industry_code
),
-- 2. total_employment: 지역별 전체 취업자 수 합계를 계산.
total_employment AS (
    SELECT region_id, SUM(avg_employed) as total_employed
    FROM region_industry_share
    GROUP BY region_id
),
-- 3. industry_concentration: HHI 지수(산업 집중도)를 계산.
--    HHI = SUM (각 산업의 점유율^2). 이 값이 클수록 특정 산업에 집중되어 있다는 의미.
industry_concentration AS (
    SELECT s.region_id, SUM(POWER(s.avg_employed * 1.0 / t.total_employed, 2)) as hhi
    FROM region_industry_share s
    JOIN total_employment t ON s.region_id = t.region_id
    GROUP BY s.region_id
)
-- 4. 최종 결과: '산업 다각화 지수' (1 - HHI)를 계산하고, 이 값이 높은 순서대로 정렬.
SELECT
    r.region_name,
    ROUND(1 - c.hhi, 4) as diversification_index,
    CASE
        WHEN (1 - c.hhi) > 0.9 THEN '매우 다각화'
        WHEN (1 - c.hhi) > 0.85 THEN '다각화'
        ELSE '집중'
    END as diversification_level
FROM industry_concentration c
JOIN dim_region r ON c.region_id = r.region_id
ORDER BY diversification_index DESC;

-- -----------------------------------------------------------------
-- [인사이트 4] 어느 지역의 고용 회복력이 가장 강한가? (코로나19 전후 비교)
-- -----------------------------------------------------------------
-- 코로나 이전(2019년) 대비 최근(2024년)의 취업자 수를 비교하여, 고용 시장이 얼마나 회복되었는지를 평가함.

-- 1. pre_covid: 코로나 이전(2019년)의 지역별 평균 취업자 수를 계산.
WITH pre_covid AS (
    SELECT region_id, AVG(employed_persons) as avg_employed_2019
    FROM fact_unemployment_monthly
    WHERE year_month LIKE '2019%'
    GROUP BY region_id
),
-- 2. post_covid: 코로나 이후(2024년)의 지역별 평균 취업자 수를 계산.
post_covid AS (
    SELECT region_id, AVG(employed_persons) as avg_employed_2024
    FROM fact_unemployment_monthly
    WHERE year_month LIKE '2024%'
    GROUP BY region_id
)
-- 3. 최종 결과: 두 시점의 취업자 수를 비교하여 회복률(%)을 계산하고, 회복률이 높은 순으로 정렬.
SELECT
    r.region_name,
    ROUND(pre.avg_employed_2019, 0) as employed_2019,
    ROUND(post.avg_employed_2024, 0) as employed_2024,
    ROUND((post.avg_employed_2024 - pre.avg_employed_2019) * 100.0 / pre.avg_employed_2019, 2) as recovery_rate_pct,
    CASE
        WHEN (post.avg_employed_2024 - pre.avg_employed_2019) * 1.0 / pre.avg_employed_2019 > 0.05 THEN '강한 회복'
        WHEN (post.avg_employed_2024 - pre.avg_employed_2019) * 1.0 / pre.avg_employed_2019 > 0 THEN '회복'
        ELSE '미회복'
    END as recovery_status
FROM pre_covid pre
JOIN post_covid post ON pre.region_id = post.region_id
JOIN dim_region r ON pre.region_id = r.region_id
ORDER BY recovery_rate_pct DESC;

-- -----------------------------------------------------------------
-- [인사이트 5] 어느 지역의 경제활동참가율이 가장 많이 증가했는가?
-- -----------------------------------------------------------------
-- 경제활동참가율(전체 인구 중 경제활동인구의 비율)의 변화를 통해, 지역의 노동 시장 활기를 간접적으로 파악함.
-- 참가율 증가는 일할 의지가 있는 사람이 늘어났다는 긍정적인 신호일 수 있음.

-- 1. yearly_participation: 지역별/연도별 평균 경제활동참가율을 계산.
WITH yearly_participation AS (
    SELECT
        u.region_id,
        CAST(SUBSTR(u.year_month, 1, 4) AS INTEGER) as year,
        AVG(u.labor_force * 100.0 / p.total_pop) as participation_rate
    FROM fact_unemployment_monthly u
    JOIN fact_population_monthly p ON u.region_id = p.region_id AND u.year_month = p.year_month
    GROUP BY u.region_id, CAST(SUBSTR(u.year_month, 1, 4) AS INTEGER)
)
-- 2. 최종 결과: 2020년과 2024년의 참가율을 비교하여 그 변화폭(rate_change)을 계산하고, 증가폭이 큰 순서로 정렬.
SELECT
    r.region_name,
    ROUND(MAX(CASE WHEN y.year = 2020 THEN y.participation_rate END), 2) as rate_2020,
    ROUND(MAX(CASE WHEN y.year = 2024 THEN y.participation_rate END), 2) as rate_2024,
    ROUND(
        MAX(CASE WHEN y.year = 2024 THEN y.participation_rate END) -
        MAX(CASE WHEN y.year = 2020 THEN y.participation_rate END),
        2
    ) as rate_change
FROM yearly_participation y
JOIN dim_region r ON y.region_id = r.region_id
WHERE y.year IN (2020, 2024)
GROUP BY r.region_id, r.region_name
HAVING MAX(CASE WHEN y.year = 2020 THEN y.participation_rate END) IS NOT NULL
   AND MAX(CASE WHEN y.year = 2024 THEN y.participation_rate END) IS NOT NULL
ORDER BY rate_change DESC;

-- -----------------------------------------------------------------
-- [인사이트 6] 어느 산업이 가장 빠르게 성장하고 있는가? (최근 3년 평균 성장률)
-- -----------------------------------------------------------------
-- 최근 3년(2022-2024)의 산업별 평균 성장률을 계산하여, 미래 유망 산업을 예측함.

WITH yearly_employment AS (
    SELECT
        industry_code,
        CAST(SUBSTR(year_month, 1, 4) AS INTEGER) as year,
        AVG(employed_persons) as avg_employed
    FROM fact_employment_by_industry_monthly
    WHERE year_month >= '2022-01'
    GROUP BY industry_code, CAST(SUBSTR(year_month, 1, 4) AS INTEGER)
),
growth_rates AS (
    SELECT
        curr.industry_code,
        curr.year,
        ((curr.avg_employed - prev.avg_employed) * 100.0 / prev.avg_employed) as growth_rate
    FROM yearly_employment curr
    LEFT JOIN yearly_employment prev
        ON curr.industry_code = prev.industry_code
        AND curr.year = prev.year + 1
    WHERE prev.avg_employed IS NOT NULL AND prev.avg_employed > 0
)
SELECT
    i.industry_name,
    ROUND(AVG(g.growth_rate), 2) as avg_growth_rate,
    COUNT(*) as years_measured,
    CASE
        WHEN AVG(g.growth_rate) > 5 THEN '고성장'
        WHEN AVG(g.growth_rate) > 2 THEN '성장'
        WHEN AVG(g.growth_rate) > -2 THEN '안정'
        ELSE '감소'
    END as growth_status
FROM growth_rates g
JOIN dim_industry i ON g.industry_code = i.industry_code
GROUP BY i.industry_code, i.industry_name
HAVING COUNT(*) >= 2
ORDER BY avg_growth_rate DESC
LIMIT 10;

-- -----------------------------------------------------------------
-- [인사이트 7] 실업률과 취업자 수 간의 상관관계 (지역별 비교)
-- -----------------------------------------------------------------
-- 지역별로 실업률과 취업자 수의 변화 추이를 분석하여, 두 지표 간의 관계를 파악함.

WITH regional_trends AS (
    SELECT
        region_id,
        year_month,
        unemployment_rate,
        employed_persons
    FROM fact_unemployment_monthly
    WHERE year_month >= '2020-01'
),
regional_stats AS (
    SELECT
        region_id,
        AVG(unemployment_rate) as avg_unemployment,
        AVG(employed_persons) as avg_employed,
        SQRT(AVG(unemployment_rate * unemployment_rate) - AVG(unemployment_rate) * AVG(unemployment_rate)) as std_unemployment,
        SQRT(AVG(employed_persons * employed_persons) - AVG(employed_persons) * AVG(employed_persons)) as std_employed
    FROM regional_trends
    GROUP BY region_id
)
SELECT
    r.region_name,
    ROUND(s.avg_unemployment, 2) as avg_unemployment_rate,
    ROUND(s.avg_employed, 0) as avg_employed_persons,
    ROUND(s.std_unemployment, 2) as unemployment_volatility,
    CASE
        WHEN s.avg_unemployment < 2.5 THEN '우수'
        WHEN s.avg_unemployment < 3.5 THEN '양호'
        ELSE '주의'
    END as employment_health
FROM regional_stats s
JOIN dim_region r ON s.region_id = r.region_id
ORDER BY s.avg_unemployment ASC;

-- -----------------------------------------------------------------
-- [인사이트 8] 월별 계절성 분석 (어느 달에 고용이 가장 활발한가?)
-- -----------------------------------------------------------------
-- 월별 평균 취업자 수를 계산하여 고용 시장의 계절적 패턴을 분석함.

WITH monthly_pattern AS (
    SELECT
        CAST(SUBSTR(year_month, 6, 2) AS INTEGER) as month,
        AVG(employed_persons) as avg_employed,
        AVG(unemployment_rate) as avg_unemployment
    FROM fact_unemployment_monthly
    WHERE year_month >= '2020-01'
    GROUP BY CAST(SUBSTR(year_month, 6, 2) AS INTEGER)
)
SELECT
    month,
    ROUND(avg_employed, 0) as avg_employed_persons,
    ROUND(avg_unemployment, 2) as avg_unemployment_rate,
    CASE
        WHEN month IN (3, 4, 9, 10) THEN '고용 성수기'
        WHEN month IN (1, 2, 12) THEN '고용 비수기'
        ELSE '보통'
    END as seasonal_pattern
FROM monthly_pattern
ORDER BY avg_employed DESC;

-- =================================================================
-- 신규 데이터 기반 심층 분석 쿼리 (인사이트 9-15)
-- =================================================================

-- -----------------------------------------------------------------
-- [인사이트 9] 고용보험 가입률 추이 분석 (지역별)
-- -----------------------------------------------------------------
-- 고용보험 가입자 수 변화를 통해 양질의 일자리 증감을 간접적으로 파악함.
-- 고용보험 가입은 정규직, 안정적인 일자리의 지표로 볼 수 있음.

WITH insurance_trends AS (
    SELECT
        region_id,
        CAST(SUBSTR(year_month, 1, 4) AS INTEGER) as year,
        AVG(insured_count) as avg_insured,
        AVG(new_insured) as avg_new_insured
    FROM fact_employment_insurance
    WHERE year_month >= '2020-01'
    GROUP BY region_id, CAST(SUBSTR(year_month, 1, 4) AS INTEGER)
),
insurance_growth AS (
    SELECT
        curr.region_id,
        curr.year,
        curr.avg_insured,
        ((curr.avg_insured - prev.avg_insured) * 100.0 / prev.avg_insured) as growth_rate
    FROM insurance_trends curr
    LEFT JOIN insurance_trends prev
        ON curr.region_id = prev.region_id
        AND curr.year = prev.year + 1
    WHERE prev.avg_insured IS NOT NULL AND prev.avg_insured > 0
)
SELECT
    r.region_name,
    ROUND(AVG(g.avg_insured), 0) as avg_insured_count,
    ROUND(AVG(g.growth_rate), 2) as avg_growth_rate,
    CASE
        WHEN AVG(g.growth_rate) > 3 THEN '빠른 증가'
        WHEN AVG(g.growth_rate) > 0 THEN '증가'
        WHEN AVG(g.growth_rate) > -3 THEN '감소'
        ELSE '빠른 감소'
    END as trend_status
FROM insurance_growth g
JOIN dim_region r ON g.region_id = r.region_id
GROUP BY r.region_id, r.region_name
HAVING COUNT(*) >= 2
ORDER BY avg_growth_rate DESC;

-- -----------------------------------------------------------------
-- [인사이트 10] 청년 고용률 분석 (20대 취업자 비중)
-- -----------------------------------------------------------------
-- 연령대별 취업자 데이터에서 20대 청년층의 고용 현황을 분석함.

WITH age_employment AS (
    SELECT
        e.region_id,
        e.age_group_id,
        AVG(e.employed_count) as avg_employed
    FROM fact_employment_by_age e
    WHERE e.year_month >= '2023-01'
    GROUP BY e.region_id, e.age_group_id
),
total_by_region AS (
    SELECT
        region_id,
        SUM(avg_employed) as total_employed
    FROM age_employment
    GROUP BY region_id
)
SELECT
    r.region_name,
    ag.age_group_name,
    ROUND(ae.avg_employed, 0) as avg_employed,
    ROUND(ae.avg_employed * 100.0 / t.total_employed, 2) as employment_share,
    CASE
        WHEN ag.age_group_id = 2 AND ae.avg_employed * 100.0 / t.total_employed > 20 THEN '청년 고용 우수'
        WHEN ag.age_group_id = 2 AND ae.avg_employed * 100.0 / t.total_employed > 15 THEN '청년 고용 양호'
        WHEN ag.age_group_id = 2 THEN '청년 고용 저조'
        ELSE '-'
    END as youth_employment_status
FROM age_employment ae
JOIN total_by_region t ON ae.region_id = t.region_id
JOIN dim_region r ON ae.region_id = r.region_id
JOIN dim_age_group ag ON ae.age_group_id = ag.age_group_id
WHERE ag.age_group_id = 2  -- 20대만 필터링
ORDER BY employment_share DESC;

-- -----------------------------------------------------------------
-- [인사이트 11] 고학력자 취업 현황 (대졸 이상 취업자 비중)
-- -----------------------------------------------------------------
-- 교육 수준별 취업자 데이터에서 대졸 이상 고학력자의 고용 현황을 분석함.

WITH edu_employment AS (
    SELECT
        e.region_id,
        e.education_id,
        AVG(e.employed_count) as avg_employed
    FROM fact_employment_by_education e
    WHERE e.year_month >= '2023-01'
    GROUP BY e.region_id, e.education_id
),
total_by_region AS (
    SELECT
        region_id,
        SUM(avg_employed) as total_employed
    FROM edu_employment
    GROUP BY region_id
)
SELECT
    r.region_name,
    ed.education_name,
    ROUND(ee.avg_employed, 0) as avg_employed,
    ROUND(ee.avg_employed * 100.0 / t.total_employed, 2) as employment_share,
    CASE
        WHEN ee.avg_employed * 100.0 / t.total_employed > 50 THEN '고학력 비중 높음'
        WHEN ee.avg_employed * 100.0 / t.total_employed > 30 THEN '고학력 비중 보통'
        ELSE '고학력 비중 낮음'
    END as education_level_status
FROM edu_employment ee
JOIN total_by_region t ON ee.region_id = t.region_id
JOIN dim_region r ON ee.region_id = r.region_id
JOIN dim_education ed ON ee.education_id = ed.education_id
WHERE ed.education_id = 4  -- 대졸 이상만 필터링
ORDER BY employment_share DESC;

-- -----------------------------------------------------------------
-- [인사이트 12] 연령대별 고용 분포 균형도
-- -----------------------------------------------------------------
-- 지역별로 연령대 간 고용이 얼마나 균형있게 분포되어 있는지 분석함.

WITH age_distribution AS (
    SELECT
        e.region_id,
        e.age_group_id,
        AVG(e.employed_count) as avg_employed
    FROM fact_employment_by_age e
    WHERE e.year_month >= '2023-01'
    GROUP BY e.region_id, e.age_group_id
),
total_by_region AS (
    SELECT
        region_id,
        SUM(avg_employed) as total_employed
    FROM age_distribution
    GROUP BY region_id
),
age_shares AS (
    SELECT
        ad.region_id,
        POWER(ad.avg_employed * 1.0 / t.total_employed, 2) as share_squared
    FROM age_distribution ad
    JOIN total_by_region t ON ad.region_id = t.region_id
),
concentration_index AS (
    SELECT
        region_id,
        SUM(share_squared) as hhi
    FROM age_shares
    GROUP BY region_id
)
SELECT
    r.region_name,
    ROUND(1 - ci.hhi, 4) as balance_index,
    CASE
        WHEN (1 - ci.hhi) > 0.75 THEN '매우 균형적'
        WHEN (1 - ci.hhi) > 0.65 THEN '균형적'
        ELSE '불균형'
    END as balance_status
FROM concentration_index ci
JOIN dim_region r ON ci.region_id = r.region_id
ORDER BY balance_index DESC;

-- -----------------------------------------------------------------
-- [인사이트 13] 교육 수준별 고용 변화 추이
-- -----------------------------------------------------------------
-- 교육 수준별로 최근 취업자 수가 어떻게 변화하고 있는지 분석함.

WITH edu_trends AS (
    SELECT
        education_id,
        CAST(SUBSTR(year_month, 1, 4) AS INTEGER) as year,
        AVG(employed_count) as avg_employed
    FROM fact_employment_by_education
    WHERE year_month >= '2020-01'
    GROUP BY education_id, CAST(SUBSTR(year_month, 1, 4) AS INTEGER)
),
edu_growth AS (
    SELECT
        curr.education_id,
        curr.year,
        ((curr.avg_employed - prev.avg_employed) * 100.0 / prev.avg_employed) as growth_rate
    FROM edu_trends curr
    LEFT JOIN edu_trends prev
        ON curr.education_id = prev.education_id
        AND curr.year = prev.year + 1
    WHERE prev.avg_employed IS NOT NULL AND prev.avg_employed > 0
)
SELECT
    ed.education_name,
    ROUND(AVG(eg.growth_rate), 2) as avg_growth_rate,
    COUNT(*) as years_measured,
    CASE
        WHEN AVG(eg.growth_rate) > 3 THEN '빠른 증가'
        WHEN AVG(eg.growth_rate) > 0 THEN '증가'
        WHEN AVG(eg.growth_rate) > -3 THEN '감소'
        ELSE '빠른 감소'
    END as trend_status
FROM edu_growth eg
JOIN dim_education ed ON eg.education_id = ed.education_id
GROUP BY ed.education_id, ed.education_name
HAVING COUNT(*) >= 2
ORDER BY avg_growth_rate DESC;

-- -----------------------------------------------------------------
-- [인사이트 14] 신규 고용보험 가입자 비율 (고용 시장 활력도)
-- -----------------------------------------------------------------
-- 전체 가입자 중 신규 가입자 비율을 통해 고용 시장의 활력을 측정함.

WITH insurance_activity AS (
    SELECT
        region_id,
        year_month,
        insured_count,
        new_insured,
        (new_insured * 100.0 / NULLIF(insured_count, 0)) as new_insured_ratio
    FROM fact_employment_insurance
    WHERE year_month >= '2023-01'
        AND insured_count > 0
        AND new_insured IS NOT NULL
)
SELECT
    r.region_name,
    ROUND(AVG(ia.insured_count), 0) as avg_total_insured,
    ROUND(AVG(ia.new_insured), 0) as avg_new_insured,
    ROUND(AVG(ia.new_insured_ratio), 2) as avg_new_ratio,
    CASE
        WHEN AVG(ia.new_insured_ratio) > 5 THEN '활발'
        WHEN AVG(ia.new_insured_ratio) > 3 THEN '보통'
        ELSE '저조'
    END as market_vitality
FROM insurance_activity ia
JOIN dim_region r ON ia.region_id = r.region_id
GROUP BY r.region_id, r.region_name
HAVING COUNT(*) >= 6
ORDER BY avg_new_ratio DESC;

-- -----------------------------------------------------------------
-- [인사이트 15] 고령층(50대 이상) 고용 비중 분석
-- -----------------------------------------------------------------
-- 고령화 사회에 대비하여 50대 이상 고령층의 고용 현황을 분석함.

WITH age_employment AS (
    SELECT
        e.region_id,
        e.age_group_id,
        AVG(e.employed_count) as avg_employed
    FROM fact_employment_by_age e
    WHERE e.year_month >= '2023-01'
    GROUP BY e.region_id, e.age_group_id
),
total_by_region AS (
    SELECT
        region_id,
        SUM(avg_employed) as total_employed
    FROM age_employment
    GROUP BY region_id
),
senior_employment AS (
    SELECT
        ae.region_id,
        SUM(ae.avg_employed) as senior_employed
    FROM age_employment ae
    WHERE ae.age_group_id >= 5  -- 50대 이상
    GROUP BY ae.region_id
)
SELECT
    r.region_name,
    ROUND(se.senior_employed, 0) as senior_employed_count,
    ROUND(t.total_employed, 0) as total_employed_count,
    ROUND(se.senior_employed * 100.0 / t.total_employed, 2) as senior_employment_share,
    CASE
        WHEN se.senior_employed * 100.0 / t.total_employed > 30 THEN '고령층 비중 높음'
        WHEN se.senior_employed * 100.0 / t.total_employed > 20 THEN '고령층 비중 보통'
        ELSE '고령층 비중 낮음'
    END as senior_employment_status
FROM senior_employment se
JOIN total_by_region t ON se.region_id = t.region_id
JOIN dim_region r ON se.region_id = r.region_id
ORDER BY senior_employment_share DESC;
