select   cast(employee_id as varchar) as employee_id,
        replace(hire_date, 'day ', '')::date as hire_date,
        name,
        city,
        address,
        title,
        cast(annual_salary as double) as annual_salary,
        _file,
        _line,
        _modified,
        _fivetran_synced
from {{ source('google_drive', 'HR_JOINS') }}