
SELECT
    j.employee_id,
    j.hire_date,
    j.name,
    j.city,
    j.address,
    j.title,
    j.annual_salary,
    q.quit_date
FROM {{ ref('base_google__hr_joins') }} j
LEFT JOIN {{ref('base_google__hr_quits')}} q
    ON j.employee_id = q.employee_id