with
    source_data as (
        select weather_data, _load_dt as loaded_at, _batch_id as batch_id
        from {{ source("raw", "weather_hourly_history") }}

    ),

    unnested as (
        select
            loaded_at, batch_id, jsonb_array_elements("weather_data"::jsonb) as record
        from source_data
    ),

    exploded as (
        select
            record ->> 'source' as source,
            record ->> 'airport_iata' as airport_iata,
            loaded_at,
            batch_id,
            jsonb_array_elements_text(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'time'
            ) as weather_time,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'temperature_2m'
            ) as temperature_2m,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'apparent_temperature'
            ) as apparent_temperature,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'dewpoint_2m'
            ) as dewpoint_2m,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'relativehumidity_2m'
            ) as relative_humidity_2m,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'precipitation'
            ) as precipitation_mm,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'rain'
            ) as rain_mm,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'snowfall'
            ) as snowfall_cm,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'cloudcover'
            ) as cloudcover_pct,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'cloudcover_low'
            ) as cloudcover_low_pct,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'cloudcover_mid'
            ) as cloudcover_mid_pct,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'cloudcover_high'
            ) as cloudcover_high_pct,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'surface_pressure'
            ) as surface_pressure_hpa,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'pressure_msl'
            ) as pressure_msl_hpa,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'visibility'
            ) as visibility_m,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'windspeed_10m'
            ) as windspeed_10m_ms,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'windgusts_10m'
            ) as windgusts_10m_ms,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'winddirection_10m'
            ) as winddirection_10m_deg,

            jsonb_array_elements(
                (record ->> 'payload')::jsonb -> 'hourly' -> 'weathercode'
            ) as weathercode
        from unnested
    ),

    typed as (
        select
            source,
            airport_iata,
            loaded_at::timestamp,

            weather_time::timestamp as weather_time_utc,

            temperature_2m::float as temperature_2m_c,
            apparent_temperature::float as apparent_temperature_c,
            dewpoint_2m::float as dewpoint_2m_c,

            nullif(relative_humidity_2m::text, 'null')::float
            as relative_humidity_2m_pct,

            precipitation_mm::float as precipitation_mm,
            rain_mm::float as rain_mm,
            snowfall_cm::float as snowfall_cm,

            nullif(cloudcover_pct::text, 'null')::int as cloudcover_pct,
            nullif(cloudcover_low_pct::text, 'null')::int as cloudcover_low_pct,
            nullif(cloudcover_mid_pct::text, 'null')::int as cloudcover_mid_pct,
            nullif(cloudcover_high_pct::text, 'null')::int as cloudcover_high_pct,

            surface_pressure_hpa::float as surface_pressure_hpa,
            pressure_msl_hpa::float as pressure_msl_hpa,

            nullif(visibility_m::text, 'null')::float as visibility_m,

            windspeed_10m_ms::float as windspeed_10m_ms,
            windgusts_10m_ms::float as windgusts_10m_ms,
            nullif(winddirection_10m_deg::text, 'null')::int as winddirection_10m_deg,

            nullif(weathercode::text, 'null')::int as weathercode
        from exploded
    ),

    final as (
        select
            *,

            case
                weathercode
                when 0
                then 'Clear sky'
                when 1
                then 'Mainly clear'
                when 2
                then 'Partly cloudy'
                when 3
                then 'Overcast'
                when 45
                then 'Fog'
                when 48
                then 'Depositing rime fog'
                when 51
                then 'Light drizzle'
                when 53
                then 'Moderate drizzle'
                when 55
                then 'Dense drizzle'
                when 56
                then 'Light freezing drizzle'
                when 57
                then 'Dense freezing drizzle'
                when 61
                then 'Slight rain'
                when 63
                then 'Moderate rain'
                when 65
                then 'Heavy rain'
                when 66
                then 'Light freezing rain'
                when 67
                then 'Heavy freezing rain'
                when 71
                then 'Slight snowfall'
                when 73
                then 'Moderate snowfall'
                when 75
                then 'Heavy snowfall'
                when 77
                then 'Snow grains'
                when 80
                then 'Slight rain showers'
                when 81
                then 'Moderate rain showers'
                when 82
                then 'Violent rain showers'
                when 85
                then 'Slight snow showers'
                when 86
                then 'Heavy snow showers'
                when 95
                then 'Thunderstorm'
                when 96
                then 'Thunderstorm with hail'
                when 99
                then 'Heavy thunderstorm with hail'
                else 'Unknown'
            end as weather_description

        from typed
    )

select *
from final
