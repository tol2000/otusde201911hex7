select nvl(DISTRICT,'00') as district,
       percentile_approx(crimes_month, 0.5) as crimes_monthly
  from (
    select row_number() over (order by count(*)) as rnum, DISTRICT,
           YEAR, MONTH,
           count(*) as crimes_month
      from crime
      group by DISTRICT, YEAR, MONTH
  ) t
  group by nvl(DISTRICT,'00')