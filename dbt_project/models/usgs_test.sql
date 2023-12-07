select * 
from {{ source('main', 'usgs_data') }}