{{ config(materialized='view') }}
SELECT
    employee_id,
    hire_date,
    name,
    city,
    address,
    title,
    annual_salary,
    quit_date,

    DATEDIFF(
        day,
        hire_date,
        COALESCE(quit_date, CURRENT_DATE)
    ) AS tenure_days
    from {{ref('int_employees')}}