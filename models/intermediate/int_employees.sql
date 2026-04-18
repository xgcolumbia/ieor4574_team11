WITH joins AS (
    SELECT
        CAST(employee_id AS VARCHAR) AS employee_id,
        REPLACE(hire_date, 'day ', '')::DATE AS hire_date,
        name,
        city,
        address,
        title,
        CAST(annual_salary AS DOUBLE) AS annual_salary
    FROM DEV.XINAN_BASE.BASE_GOOGLE__HR_JOINS),

quits AS (
    SELECT
        CAST(employee_id AS VARCHAR) AS employee_id,
        TRY_TO_DATE(quit_date) AS quit_date
    FROM {{ source('google_drive', 'HR_QUITS') }}
    WHERE TRY_TO_DATE(quit_date) IS NOT NULL
        AND employee_id IS NOT NULL
)

SELECT
    j.employee_id,
    j.hire_date,
    j.name,
    j.city,
    j.address,
    j.title,
    j.annual_salary,
    q.quit_date
FROM joins j
LEFT JOIN quits q
    ON j.employee_id = q.employee_id