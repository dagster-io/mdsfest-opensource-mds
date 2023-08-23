select

    year(date_trunc('year', obs_date)) as year,
    bird_name,
    country,
    region,
    sum(species_count) as species_count

from {{ ref('all_birds') }}
group by all
order by year, species_count desc
