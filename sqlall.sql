select tot.district, tot.crimes_total, freq.frequent_crime_types, crimes_monthly, tot.lat, tot.lng
  from tot
    join freq on tot.district = freq.district
    join monthly on tot.district = monthly.district
  order by substr(tot.district,1,1), int(substr(tot.district,2))