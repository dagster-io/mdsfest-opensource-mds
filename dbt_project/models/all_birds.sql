select

birds.loc_id,
birds.subnational1_code as region,
make_date(year, month, day) as obs_date,
birds.species_code,
try_cast(birds.how_many as integer) as species_count,
birds.valid as is_valid,
birds.reviewed as is_reviewed,
species.american_english_name,
sites.nearby_feeders,
sites.population_atleast,
sites.count_area_size_sq_m_atleast,

from {{ source('main', 'birds') }}
join {{ source('main', 'sites') }} using (loc_id)
join {{ source('main', 'species') }} using (species_code)
where try_cast(is_valid as boolean) = true
