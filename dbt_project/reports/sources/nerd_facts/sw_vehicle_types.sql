SELECT
    'Starships' as vehicle_type,
    COUNT(*) as count
FROM
    public.stg_swapi_starships
UNION ALL
SELECT
    'Vehicles' as vehicle_type,
    COUNT(*) as count
FROM
    public.stg_swapi_vehicles
ORDER BY
    count DESC;