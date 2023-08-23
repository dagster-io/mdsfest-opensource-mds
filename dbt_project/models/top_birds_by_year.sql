select

    year(date_trunc('year', obs_date)) as year,
    american_english_name,
    region,
    sum(species_count) as species_count

from {{ ref('all_birds') }}
group by all
order by year, species_count desc
