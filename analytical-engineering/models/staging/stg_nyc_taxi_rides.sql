with 
source as (
    select * from {{source('staging', 'nyc_ride_partitioned_clustered')}}
),

select * from source