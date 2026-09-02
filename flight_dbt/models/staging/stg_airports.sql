select
    -- названия и география (text)
    nullif(nullif(trim("Name"), ''), '\N')::text as airport_name,
    nullif(nullif(trim("City"), ''), '\N')::text as airport_city,
    nullif(nullif(trim("Country"), ''), '\N')::text as airport_country,

    -- коды аэропортов
    case
        when upper(trim("IATA")) in ('', '\N') then null else upper(trim("IATA"))
    end::text as airport_iata,

    case
        when upper(trim("ICAO")) in ('', '\N') then null else upper(trim("ICAO"))
    end::text as airport_icao,

    -- координаты
    case
        when trim("Latitude") ~ '^-?[0-9]+(\.[0-9]+)?$'
        then trim("Latitude")::numeric
        else null
    end as airport_latitude,

    case
        when trim("Longitude") ~ '^-?[0-9]+(\.[0-9]+)?$'
        then trim("Longitude")::numeric
        else null
    end as airport_longitude,

    case
        when trim("Altitude") ~ '^-?[0-9]+(\.[0-9]+)?$'
        then trim("Altitude")::numeric
        else null
    end as airport_altitude,

    -- временные зоны
    nullif(nullif(trim("Timezone"), ''), '\N')::text as airport_timezone_offset,
    nullif(nullif(trim("Timezone_1"), ''), '\N')::text as airport_timezone,

    -- техполе из источника
    _load_dt as source_loaded_at

from {{ source("raw", "airports") }}
