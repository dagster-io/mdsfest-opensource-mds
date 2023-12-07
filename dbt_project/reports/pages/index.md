# Open-Source Modern Data Stack

This is the visualization output from the Open-Source MDS Pipeline.

![Asset Graph](/asset_graph.png)

See the [About](/about) page for more information about the project.

## River Data

```sql all_river
select *
from test
limit 500
```

## Bird Data

```sql all_birds
select *
from all_birds
limit 500
```

<DataTable data="{all_birds}" search="true" />

```sql count_birds
select
date_trunc('year', obs_date) as obs_year,
count(distinct checklist_id) as n_checklists,
count(distinct observation_id) as n_observations,
sum(species_count) as total_species

from all_birds
group by all
order by 1 desc
```

## Top Toots

```sql top_toots
select *
from top_toots
```

<BarChart
    data={top_toots}
    swapXY=true
    x=hashtag
    y=count
    xType=category
/>


## Annual Summary Metrics

<BigValue
    data={count_birds}
    value='n_checklists'
    title='# Checklists'
    sparkline='obs_year'
    maxWidth='14em'
/>


<BigValue
    data={count_birds}
    value='n_observations'
    title='# Observations'
    sparkline='obs_year'
    maxWidth='14em'
/>


<BigValue
    data={count_birds}
    value='total_species'
    title='# Total Species'
    sparkline='obs_year'
    maxWidth='14em'
/>

## California Quail

![California Quail](https://cdn.download.ams.birds.cornell.edu/api/v1/asset/562190741/900)

```sql quail
select
date_trunc('month', obs_date) as obs_month,
region,
sum(species_count) as species_count
from all_birds
where bird_name = 'California Quail'
group by all
```

<LineChart
    data={quail}
    x='obs_month'
    y='species_count'
    series='region'
/>



## Some Regional Birds

```sql top_states
select
country,
region,
sum(species_count) as total_count
from all_birds

group by all
order by 3 desc
limit 10
```

<BarChart
    data={top_states}
    swapXY=true
    x=country
    y=total_count
    series=region
    xType=category
    sort=false
/>

```sql regional_birds
select
country,
region,
bird_name,
total_count from (
    select

    country,
    region,
    bird_name,
    sum(species_count) as total_count,
    row_number() over (partition by region order by sum(species_count) desc) as rank

    from all_birds
    where region in ('CA', 'ON', 'NY', 'PA', 'VA')
    group by 1,2,3
) where rank <= 5

```
<BarChart
    data={regional_birds}
    swapXY=true
    x=region
    y=total_count
    series=bird_name
    xType=category
    sort=false
/>


