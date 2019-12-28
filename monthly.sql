select nvl(DISTRICT,'00') as district,
       percentile_approx(crimes_month, 0.50001) as crimes_monthly
  from (
    select row_number() over (order by count(*)) as rnum, DISTRICT,
           YEAR, MONTH,
           count(*) as crimes_month
      from crime
      where DISTRICT='C6'
      group by DISTRICT, YEAR, MONTH
      order by rnum
  ) t
  group by nvl(DISTRICT,'00')