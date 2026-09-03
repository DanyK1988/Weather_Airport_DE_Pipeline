{{
    config(
        materialized="incremental",
        unique_key="flight_key",
        incremental_strategy="merge",
    )
}}

with
    arrivals_api as (

        select

            flight_number,
            call_sign,
            flight_status,
            is_cargo,
            direction as flight_direction,

            airline_iata,
            airline_icao,

            aircraft_model,
            aircraft_registration,

            departure_airport_iata as airport_departure_iata,
            airport_code as airport_arrival_iata,

            scheduled_departure_utc,
            revised_departure_utc as actual_departure_utc,

            scheduled_arrival_utc,
            revised_arrival_utc as actual_arrival_utc

        from {{ ref("stg_aerodatabox_arrivals") }}

    ),

    arrivals_parquet as (

        select

            flight_number,
            call_sign,
            flight_status,
            is_cargo,
            direction as flight_direction,

            airline_iata,
            airline_icao,

            aircraft_model,
            aircraft_registration,

            departure_airport_iata as airport_departure_iata,
            airport_code as airport_arrival_iata,

            scheduled_departure_utc,
            revised_departure_utc as actual_departure_utc,

            scheduled_arrival_utc,
            revised_arrival_utc as actual_arrival_utc

        from {{ ref("stg_parquet_arrivals") }}

    ),

    departures_api as (

        select

            flight_number,
            call_sign,
            flight_status,
            is_cargo,
            direction as flight_direction,

            airline_iata,
            airline_icao,

            aircraft_model,
            aircraft_registration,

            airport_code as airport_departure_iata,
            arrival_airport_iata as airport_arrival_iata,

            scheduled_departure_utc,
            revised_departure_utc as actual_departure_utc,

            scheduled_arrival_utc,
            revised_arrival_utc as actual_arrival_utc

        from {{ ref("stg_aerodatabox_departures") }}

    ),

    departures_parquet as (

        select

            flight_number,
            call_sign,
            flight_status,
            is_cargo,
            direction as flight_direction,

            airline_iata,
            airline_icao,

            aircraft_model,
            aircraft_registration,

            airport_code as airport_departure_iata,
            arrival_airport_iata as airport_arrival_iata,

            scheduled_departure_utc,
            revised_departure_utc as actual_departure_utc,

            scheduled_arrival_utc,
            revised_arrival_utc as actual_arrival_utc

        from {{ ref("stg_parquet_departures") }}

    ),

    arrivals_union as (

        select *
        from arrivals_api

        union all

        select *
        from arrivals_parquet

    ),

    departures_union as (

        select *
        from departures_api

        union all

        select *
        from departures_parquet

    ),

    flights_union as (

        select *
        from arrivals_union

        union all

        select *
        from departures_union

    ),

    deduped as (

        select *

        from
            (

                select
                    *,

                    row_number() over (
                        partition by
                            airline_iata,
                            flight_number,
                            coalesce(scheduled_departure_utc, scheduled_arrival_utc),
                            airport_departure_iata,
                            airport_arrival_iata,
                            flight_direction

                        order by
                            actual_departure_utc desc nulls last,
                            actual_arrival_utc desc nulls last
                    ) as rn

                from flights_union

            ) t

        where rn = 1

    ),

    final as (

        select

            md5(
                coalesce(airline_iata, '')
                || coalesce(flight_number, '')
                || coalesce(
                    cast(
                        coalesce(scheduled_departure_utc, scheduled_arrival_utc) as text
                    ),
                    ''
                )
                || coalesce(airport_departure_iata, '')
                || coalesce(airport_arrival_iata, '')
                || coalesce(flight_direction, '')
            ) as flight_key,

            flight_number,
            call_sign,
            flight_status,
            is_cargo,
            flight_direction,

            airline_iata,
            airline_icao,

            aircraft_model,
            aircraft_registration,

            airport_departure_iata,
            airport_arrival_iata,

            scheduled_departure_utc,
            actual_departure_utc,

            scheduled_arrival_utc,
            actual_arrival_utc,

            case
                when
                    actual_departure_utc is not null
                    and scheduled_departure_utc is not null

                then
                    round(
                        extract(
                            epoch from (actual_departure_utc - scheduled_departure_utc)
                        )
                        / 60.0,
                        2
                    )
            end as departure_delay_minutes,

            case
                when
                    actual_arrival_utc is not null and scheduled_arrival_utc is not null

                then
                    round(
                        extract(epoch from (actual_arrival_utc - scheduled_arrival_utc))
                        / 60.0,
                        2
                    )
            end as arrival_delay_minutes,

            case
                when flight_direction = 'departure'
                then scheduled_departure_utc

                when flight_direction = 'arrival'
                then scheduled_arrival_utc
            end as operation_ts,

            case
                when flight_direction = 'departure'
                then date(scheduled_departure_utc)

                when flight_direction = 'arrival'
                then date(scheduled_arrival_utc)
            end as operation_date,

            extract(
                hour
                from

                    case
                        when flight_direction = 'departure'
                        then scheduled_departure_utc

                        when flight_direction = 'arrival'
                        then scheduled_arrival_utc
                    end
            ) as operation_hour,

            to_char(

                case
                    when flight_direction = 'departure'
                    then scheduled_departure_utc

                    when flight_direction = 'arrival'
                    then scheduled_arrival_utc
                end,

                'Day'

            ) as operation_day_of_week

        from deduped

    )

select

    *,

    case
        when departure_delay_minutes is null
        then 'unknown'

        when departure_delay_minutes <= 0
        then 'on_time'

        when departure_delay_minutes <= 15
        then 'minor_delay'

        else 'delayed'
    end as departure_time_status,

    case
        when arrival_delay_minutes is null
        then 'unknown'

        when arrival_delay_minutes <= 0
        then 'on_time'

        when arrival_delay_minutes <= 15
        then 'minor_delay'

        else 'delayed'
    end as arrival_time_status

from final
