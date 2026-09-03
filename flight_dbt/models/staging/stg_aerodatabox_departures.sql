with
    source_data as (

        select
            source::text as source,
            airport_code::text as airport_code,
            code_type::text as code_type,
            from_local::timestamptz as window_from_local,
            to_local::timestamptz as window_to_local,
            extracted_at::timestamp as loaded_at,
            payload::jsonb as payload

        from {{ source("raw", "aerodatabox_fids_range") }}

    ),

    departures as (

        select
            source,
            airport_code,
            code_type,
            window_from_local,
            window_to_local,
            loaded_at,
            jsonb_array_elements(payload -> 'departures')::jsonb as departure

        from source_data
        where payload ? 'departures'

    ),

    final as (

        select
            source,
            airport_code,
            code_type,
            window_from_local,
            window_to_local,
            loaded_at,
            'departure'::text as direction,
            (departure ->> 'number')::text as flight_number,
            (departure ->> 'status')::text as flight_status,
            (departure ->> 'callSign')::text as call_sign,
            (departure ->> 'codeshareStatus')::text as codeshare_status,
            (departure -> 'airline' ->> 'iata')::text as airline_iata,
            (departure -> 'airline' ->> 'icao')::text as airline_icao,
            (departure -> 'airline' ->> 'name')::text as airline_name,
            (departure -> 'aircraft' ->> 'reg')::text as aircraft_registration,
            (departure -> 'aircraft' ->> 'modeS')::text as aircraft_modes,
            (departure -> 'aircraft' ->> 'model')::text as aircraft_model,
            (departure -> 'departure' ->> 'terminal')::text as departure_terminal,
            (departure -> 'departure' -> 'scheduledTime' ->> 'utc')::timestamp
            as scheduled_departure_utc,
            (departure -> 'departure' -> 'scheduledTime' ->> 'local')::timestamptz
            as scheduled_departure_local,
            (departure -> 'departure' -> 'revisedTime' ->> 'utc')::timestamp
            as revised_departure_utc,
            (departure -> 'departure' -> 'revisedTime' ->> 'local')::timestamptz
            as revised_departure_local,
            (departure -> 'arrival' -> 'airport' ->> 'iata')::text
            as arrival_airport_iata,
            (departure -> 'arrival' -> 'airport' ->> 'icao')::text
            as arrival_airport_icao,
            (departure -> 'arrival' -> 'airport' ->> 'name')::text
            as arrival_airport_city,
            (departure -> 'arrival' -> 'airport' ->> 'timeZone')::text
            as arrival_airport_timezone,
            (departure -> 'arrival' ->> 'terminal')::text as arrival_terminal,
            (departure -> 'arrival' -> 'scheduledTime' ->> 'utc')::timestamp
            as scheduled_arrival_utc,
            (departure -> 'arrival' -> 'scheduledTime' ->> 'local')::timestamptz
            as scheduled_arrival_local,
            (departure -> 'arrival' -> 'revisedTime' ->> 'utc')::timestamp
            as revised_arrival_utc,
            (departure -> 'arrival' -> 'revisedTime' ->> 'local')::timestamptz
            as revised_arrival_local,
            (departure ->> 'isCargo')::boolean as is_cargo,
            departure::jsonb as departure_payload

        from departures

    )

select *
from final
