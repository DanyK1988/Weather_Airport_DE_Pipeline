{{
    config(
        materialized="incremental",
        unique_key="weather_key",
        incremental_strategy="merge",
    )
}}

with
    weather_union as (
        select *
        from {{ ref("stg_weather_api") }}

        union all

        select *
        from {{ ref("stg_weather_history") }}
    ),

    deduped as (
        select *

        from
            (
                select
                    *,
                    row_number() over (
                        partition by airport_iata, weather_time_utc
                        order by weather_time_utc desc
                    ) as rn

                from weather_union
            ) t

        where rn = 1
    )

select
    {{ generate_surrogate_key(["airport_iata", "weather_time_utc"]) }} as weather_key,
    airport_iata,
    weather_time_utc,
    temperature_2m_c::numeric as temperature_2m_c,
    apparent_temperature_c::numeric as apparent_temperature_c,
    dewpoint_2m_c::numeric as dewpoint_2m_c,
    relative_humidity_2m_pct::numeric as relative_humidity_2m_pct,
    precipitation_mm::numeric as precipitation_mm,
    rain_mm::numeric as rain_mm,
    snowfall_cm::numeric as snowfall_cm,
    cloudcover_pct::numeric as cloudcover_pct,
    visibility_m::numeric as visibility_m,
    windspeed_10m_ms::numeric as windspeed_10m_ms,
    winddirection_10m_deg::numeric as winddirection_10m_deg,
    weathercode::int as weathercode,
    weather_description,
    date(weather_time_utc) as weather_date,
    extract(hour from weather_time_utc) as weather_hour,
    to_char(weather_time_utc, 'Day') as weather_day_of_week,

    -- Категории ветра
    case
        when windspeed_10m_ms::numeric >= 13.9
        then 'strong'
        when windspeed_10m_ms::numeric >= 10.8
        then 'moderate'
        when windspeed_10m_ms::numeric >= 5.5
        then 'light'
        else 'calm'
    end as wind_category,

    -- Категория осаднов
    {{
        range_category(
            "precipitation_mm::numeric",
            [
                {"operator": ">=", "value": 10, "label": "heavy"},
                {"operator": ">=", "value": 2.5, "label": "moderate"},
                {"operator": ">", "value": 0, "label": "light"},
            ],
            "none",
        )
    }} as precipitation_category,

    -- Категория дождя
    case
        when rain_mm::numeric >= 10
        then 'heavy_rain'
        when rain_mm::numeric >= 2.5
        then 'moderate_rain'
        when rain_mm::numeric > 0
        then 'light_rain'
        else 'no_rain'
    end as rain_category,

    -- категория температуры
    case
        when temperature_2m_c::numeric >= 35
        then 'extreme_heat'
        when temperature_2m_c::numeric >= 30
        then 'very_hot'
        when temperature_2m_c::numeric >= 25
        then 'hot'
        when temperature_2m_c::numeric >= 20
        then 'warm'
        else 'mild'
    end as temperature_category,

    -- категории облачности
    case
        when cloudcover_pct::numeric >= 90
        then 'overcast'
        when cloudcover_pct::numeric >= 70
        then 'mostly_cloudy'
        when cloudcover_pct::numeric >= 40
        then 'partly_cloudy'
        when cloudcover_pct::numeric >= 10
        then 'mostly_clear'
        else 'clear'
    end as cloudcover_category,

    -- категории видимости
    {{
        range_category(
            "visibility_m::numeric",
            [
                {"operator": "<", "value": 1000, "label": "very_poor"},
                {"operator": "<", "value": 3000, "label": "poor"},
                {"operator": "<", "value": 5000, "label": "moderate"},
                {"operator": "<", "value": 10000, "label": "good"},
            ],
            "excellent",
        )
    }} as visibility_category,

    windspeed_10m_ms::numeric >= 13.9 as is_strong_wind,
    precipitation_mm::numeric >= 10 as is_heavy_precipitation,
    rain_mm::numeric >= 10 as is_heavy_rain,
    weathercode::integer in (95, 96, 99) as has_thunderstorm,
    cloudcover_pct::numeric >= 90 as is_overcast,
    visibility_m::numeric < 3000 as is_low_visibility

from deduped
