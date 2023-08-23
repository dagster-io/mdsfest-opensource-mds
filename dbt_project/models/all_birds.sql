select

birds.sub_id as checklist_id,
birds.obs_id as observation_id,
birds.loc_id,
birds.latitude,
birds.longitude,

left(birds.subnational1_code, 2) as country,
right(birds.subnational1_code, 2) as region,
make_date(year, month, day) as obs_date,
birds.species_code,
try_cast(birds.how_many as integer) as species_count,
birds.valid as is_valid,
birds.reviewed as is_reviewed,
species.american_english_name as bird_name,
sites.nearby_feeders,

case try_cast(sites.population_atleast as numeric)
    when 1 then 'a. <5,000'
    when 2 then 'b. 5,0001 - 25,000'
    when 3 then 'c. 25,000 - 100,000'
    when 4 then 'd. >100,000'
end as population_city,

case sites.count_area_size_sq_m_atleast
    when '0.01' then 'a. <1 sq. m'
    when '1.01' then 'b. 1 - 100 sq. m'
    when '100.01' then 'c. 100 - 375 sq. m'
    when '375.01' then 'd. >375 sq. m'
end as survey_site_size

from {{ source('main', 'birds') }}
join {{ source('main', 'sites') }} using (loc_id, proj_period_id)
join {{ source('main', 'species') }} using (species_code)
where try_cast(is_valid as boolean) = true
