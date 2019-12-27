select district, crime_type
  from (
    select district,
           crime_type,
           row_number() over (partition by district order by crimes_count desc) as crime_type_pos
      from (
        select nvl(cr.DISTRICT,'00') as district,
               split(co.NAME,' - ')[0] as crime_type,
               count(*) as crimes_count
          from crime cr
            join codes co on ( int(cr.OFFENSE_CODE) = int(co.CODE) )
          group by nvl(cr.DISTRICT,'00'), split(co.NAME,' - ')[0]
      )
  )
  where crime_type_pos <= 3
  order by district, crime_type_pos