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

        from {{ get_raw_source() }}
    ),

    arrivals as (
        select
            source,
            airport_code,
            code_type,
            window_from_local,
            window_to_local,
            loaded_at,

            jsonb_array_elements(payload -> 'arrivals')::jsonb as arrival

        from source_data

        where payload ? 'arrivals'
    ),

    typed as (
        select
            source,
            airport_code,
            code_type,

            window_from_local,
            window_to_local,
            loaded_at,

            'arrival'::text as direction,

            (arrival ->> 'number')::text as flight_number,
            (arrival ->> 'status')::text as flight_status,
            (arrival ->> 'callSign')::text as call_sign,
            (arrival ->> 'codeshareStatus')::text as codeshare_status,
            (arrival -> 'airline' ->> 'iata')::text as airline_iata,
            (arrival -> 'airline' ->> 'icao')::text as airline_icao,
            (arrival -> 'airline' ->> 'name')::text as airline_name,
            (arrival -> 'aircraft' ->> 'reg')::text as aircraft_registration,
            (arrival -> 'aircraft' ->> 'modeS')::text as aircraft_modes,
            (arrival -> 'aircraft' ->> 'model')::text as aircraft_model,
            (arrival -> 'arrival' ->> 'terminal')::text as arrival_terminal,
            (arrival -> 'arrival' -> 'scheduledTime' ->> 'utc')::timestamp
            as scheduled_arrival_utc,
            (arrival -> 'arrival' -> 'scheduledTime' ->> 'local')::timestamptz
            as scheduled_arrival_local,
            (arrival -> 'arrival' -> 'revisedTime' ->> 'utc')::timestamp
            as revised_arrival_utc,
            (arrival -> 'arrival' -> 'revisedTime' ->> 'local')::timestamptz
            as revised_arrival_local,
            (arrival -> 'departure' -> 'airport' ->> 'iata')::text
            as departure_airport_iata,
            (arrival -> 'departure' -> 'airport' ->> 'icao')::text
            as departure_airport_icao,
            (arrival -> 'departure' -> 'airport' ->> 'name')::text
            as departure_airport_city,
            (arrival -> 'departure' -> 'airport' ->> 'timeZone')::text
            as departure_airport_timezone,
            (arrival -> 'departure' ->> 'terminal')::text as departure_terminal,
            (arrival -> 'departure' -> 'scheduledTime' ->> 'utc')::timestamp
            as scheduled_departure_utc,
            (arrival -> 'departure' -> 'scheduledTime' ->> 'local')::timestamptz
            as scheduled_departure_local,
            (arrival -> 'departure' -> 'revisedTime' ->> 'utc')::timestamp
            as revised_departure_utc,
            (arrival -> 'departure' -> 'revisedTime' ->> 'local')::timestamptz
            as revised_departure_local,
            (arrival ->> 'isCargo')::boolean as is_cargo,
            arrival::jsonb as arrival_payload
        from arrivals
    ),

    final as (select * from typed)

select *
from final
