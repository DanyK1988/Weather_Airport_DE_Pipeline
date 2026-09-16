{{ config(materialized="table") }}

with
    source_data as (

        select
            source,
            airport_code,
            code_type,
            from_iso8601_timestamp(replace(from_local, ' ', 'T')) as window_from_local,
            from_iso8601_timestamp(replace(to_local, ' ', 'T')) as window_to_local,
            cast(extracted_at as timestamp) as loaded_at,
            json_parse(payload) as payload

        from {{ source("flights_raw", "fids_range") }}

    ),

    departures as (

        select
            source,
            airport_code,
            code_type,
            window_from_local,
            window_to_local,
            loaded_at,
            flight

        from source_data
        cross join
            unnest(cast(json_extract(payload, '$.departures') as array(json))) as t(
                flight
            )
        where json_extract(payload, '$.departures') is not null

    ),

    final as (

        select
            source,
            airport_code,
            code_type,
            window_from_local,
            window_to_local,
            loaded_at,

            'departure' as direction,

            json_extract_scalar(flight, '$.number') as flight_number,
            json_extract_scalar(flight, '$.status') as flight_status,
            json_extract_scalar(flight, '$.callSign') as call_sign,
            json_extract_scalar(flight, '$.codeshareStatus') as codeshare_status,

            json_extract_scalar(flight, '$.airline.iata') as airline_iata,
            json_extract_scalar(flight, '$.airline.icao') as airline_icao,
            json_extract_scalar(flight, '$.airline.name') as airline_name,

            json_extract_scalar(flight, '$.aircraft.reg') as aircraft_registration,
            json_extract_scalar(flight, '$.aircraft.modeS') as aircraft_modes,
            json_extract_scalar(flight, '$.aircraft.model') as aircraft_model,

            json_extract_scalar(flight, '$.departure.terminal') as departure_terminal,

            cast(
                replace(
                    json_extract_scalar(flight, '$.departure.scheduledTime.utc'),
                    'Z',
                    ''
                ) as timestamp
            ) as scheduled_departure_utc,
            from_iso8601_timestamp(
                replace(
                    json_extract_scalar(flight, '$.departure.scheduledTime.local'),
                    ' ',
                    'T'
                )
            ) as scheduled_departure_local,

            cast(
                replace(
                    json_extract_scalar(flight, '$.departure.revisedTime.utc'), 'Z', ''
                ) as timestamp
            ) as revised_departure_utc,
            from_iso8601_timestamp(
                replace(
                    json_extract_scalar(flight, '$.departure.revisedTime.local'),
                    ' ',
                    'T'
                )
            ) as revised_departure_local,

            json_extract_scalar(
                flight, '$.arrival.airport.iata'
            ) as arrival_airport_iata,
            json_extract_scalar(
                flight, '$.arrival.airport.icao'
            ) as arrival_airport_icao,
            json_extract_scalar(
                flight, '$.arrival.airport.name'
            ) as arrival_airport_city,
            json_extract_scalar(
                flight, '$.arrival.airport.timeZone'
            ) as arrival_airport_timezone,
            json_extract_scalar(flight, '$.arrival.terminal') as arrival_terminal,

            cast(
                replace(
                    json_extract_scalar(flight, '$.arrival.scheduledTime.utc'), 'Z', ''
                ) as timestamp
            ) as scheduled_arrival_utc,
            from_iso8601_timestamp(
                replace(
                    json_extract_scalar(flight, '$.arrival.scheduledTime.local'),
                    ' ',
                    'T'
                )
            ) as scheduled_arrival_local,

            cast(
                replace(
                    json_extract_scalar(flight, '$.arrival.revisedTime.utc'), 'Z', ''
                ) as timestamp
            ) as revised_arrival_utc,
            from_iso8601_timestamp(
                replace(
                    json_extract_scalar(flight, '$.arrival.revisedTime.local'), ' ', 'T'
                )
            ) as revised_arrival_local,

            cast(json_extract_scalar(flight, '$.isCargo') as boolean) as is_cargo,

            json_format(flight) as departure_payload

        from departures

    )

select *
from final
