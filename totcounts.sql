select nvl(DISTRICT,'00') as district,
       count(*) as crimes_total,
       avg(Lat) as lat,
       avg(Long) as lng
  from crime
  group by DISTRICT
