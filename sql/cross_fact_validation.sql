-- =================================================================
-- Cross-Fact Validation: 팩트 테이블 간 데이터 일관성 검증
-- =================================================================
-- 목적: 서로 다른 Fact 테이블의 취업자 수가 논리적으로 일치하는지 확인

-- -----------------------------------------------------------------
-- 1. 산업별 취업자 수 합계 vs 전체 취업자 수 비교
-- -----------------------------------------------------------------
-- fact_unemployment_monthly의 employed_persons (전체 취업자)와
-- fact_employment_by_industry_monthly의 산업별 합계가 일치하는지 확인

WITH industry_total AS (
    SELECT
        region_id,
        year_month,
        SUM(employed_persons) as industry_sum
    FROM fact_employment_by_industry_monthly
    GROUP BY region_id, year_month
),
unemployment_total AS (
    SELECT
        region_id,
        year_month,
        employed_persons as unemployment_employed
    FROM fact_unemployment_monthly
)
SELECT
    r.region_name,
    i.year_month,
    u.unemployment_employed as total_employed_unemployment_table,
    i.industry_sum as total_employed_industry_sum,
    (u.unemployment_employed - i.industry_sum) as difference,
    ROUND(ABS(u.unemployment_employed - i.industry_sum) * 100.0 / u.unemployment_employed, 2) as difference_pct
FROM industry_total i
JOIN unemployment_total u
    ON i.region_id = u.region_id AND i.year_month = u.year_month
JOIN dim_region r
    ON i.region_id = r.region_id
WHERE ABS(u.unemployment_employed - i.industry_sum) > 0  -- 차이가 있는 경우만
ORDER BY difference_pct DESC, i.year_month DESC
LIMIT 50;


-- -----------------------------------------------------------------
-- 2. 학력별 취업자 수 합계 vs 전체 취업자 수 비교
-- -----------------------------------------------------------------
WITH education_total AS (
    SELECT
        region_id,
        year_month,
        SUM(employed_count) as education_sum
    FROM fact_employment_by_education
    GROUP BY region_id, year_month
),
unemployment_total AS (
    SELECT
        region_id,
        year_month,
        employed_persons as unemployment_employed
    FROM fact_unemployment_monthly
)
SELECT
    r.region_name,
    e.year_month,
    u.unemployment_employed as total_employed_unemployment_table,
    e.education_sum as total_employed_education_sum,
    (u.unemployment_employed - e.education_sum) as difference,
    ROUND(ABS(u.unemployment_employed - e.education_sum) * 100.0 / u.unemployment_employed, 2) as difference_pct
FROM education_total e
JOIN unemployment_total u
    ON e.region_id = u.region_id AND e.year_month = u.year_month
JOIN dim_region r
    ON e.region_id = r.region_id
WHERE ABS(u.unemployment_employed - e.education_sum) > 0  -- 차이가 있는 경우만
ORDER BY difference_pct DESC, e.year_month DESC
LIMIT 50;


-- -----------------------------------------------------------------
-- 3. 연령대별 취업자 수 합계 vs 전체 취업자 수 비교
-- -----------------------------------------------------------------
WITH age_total AS (
    SELECT
        region_id,
        year_month,
        SUM(employed_count) as age_sum
    FROM fact_employment_by_age
    GROUP BY region_id, year_month
),
unemployment_total AS (
    SELECT
        region_id,
        year_month,
        employed_persons as unemployment_employed
    FROM fact_unemployment_monthly
)
SELECT
    r.region_name,
    a.year_month,
    u.unemployment_employed as total_employed_unemployment_table,
    a.age_sum as total_employed_age_sum,
    (u.unemployment_employed - a.age_sum) as difference,
    ROUND(ABS(u.unemployment_employed - a.age_sum) * 100.0 / u.unemployment_employed, 2) as difference_pct
FROM age_total a
JOIN unemployment_total u
    ON a.region_id = u.region_id AND a.year_month = u.year_month
JOIN dim_region r
    ON a.region_id = r.region_id
WHERE ABS(u.unemployment_employed - a.age_sum) > 0  -- 차이가 있는 경우만
ORDER BY difference_pct DESC, a.year_month DESC
LIMIT 50;


-- -----------------------------------------------------------------
-- 4. 전체 통계 요약
-- -----------------------------------------------------------------
SELECT
    'Industry vs Unemployment' as comparison_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN ABS(diff_pct) < 1 THEN 1 ELSE 0 END) as within_1_pct,
    SUM(CASE WHEN ABS(diff_pct) < 5 THEN 1 ELSE 0 END) as within_5_pct,
    SUM(CASE WHEN ABS(diff_pct) >= 5 THEN 1 ELSE 0 END) as over_5_pct,
    ROUND(AVG(ABS(diff_pct)), 2) as avg_diff_pct,
    ROUND(MAX(ABS(diff_pct)), 2) as max_diff_pct
FROM (
    SELECT
        ABS(u.employed_persons - i.industry_sum) * 100.0 / u.employed_persons as diff_pct
    FROM (
        SELECT region_id, year_month, SUM(employed_persons) as industry_sum
        FROM fact_employment_by_industry_monthly
        GROUP BY region_id, year_month
    ) i
    JOIN fact_unemployment_monthly u
        ON i.region_id = u.region_id AND i.year_month = u.year_month
)

UNION ALL

SELECT
    'Education vs Unemployment' as comparison_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN ABS(diff_pct) < 1 THEN 1 ELSE 0 END) as within_1_pct,
    SUM(CASE WHEN ABS(diff_pct) < 5 THEN 1 ELSE 0 END) as within_5_pct,
    SUM(CASE WHEN ABS(diff_pct) >= 5 THEN 1 ELSE 0 END) as over_5_pct,
    ROUND(AVG(ABS(diff_pct)), 2) as avg_diff_pct,
    ROUND(MAX(ABS(diff_pct)), 2) as max_diff_pct
FROM (
    SELECT
        ABS(u.employed_persons - e.education_sum) * 100.0 / u.employed_persons as diff_pct
    FROM (
        SELECT region_id, year_month, SUM(employed_count) as education_sum
        FROM fact_employment_by_education
        GROUP BY region_id, year_month
    ) e
    JOIN fact_unemployment_monthly u
        ON e.region_id = u.region_id AND e.year_month = u.year_month
)

UNION ALL

SELECT
    'Age vs Unemployment' as comparison_type,
    COUNT(*) as total_records,
    SUM(CASE WHEN ABS(diff_pct) < 1 THEN 1 ELSE 0 END) as within_1_pct,
    SUM(CASE WHEN ABS(diff_pct) < 5 THEN 1 ELSE 0 END) as within_5_pct,
    SUM(CASE WHEN ABS(diff_pct) >= 5 THEN 1 ELSE 0 END) as over_5_pct,
    ROUND(AVG(ABS(diff_pct)), 2) as avg_diff_pct,
    ROUND(MAX(ABS(diff_pct)), 2) as max_diff_pct
FROM (
    SELECT
        ABS(u.employed_persons - a.age_sum) * 100.0 / u.employed_persons as diff_pct
    FROM (
        SELECT region_id, year_month, SUM(employed_count) as age_sum
        FROM fact_employment_by_age
        GROUP BY region_id, year_month
    ) a
    JOIN fact_unemployment_monthly u
        ON a.region_id = u.region_id AND a.year_month = u.year_month
);
