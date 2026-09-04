{% test valid_delay_status(model, delay_column, status_column) %}

    select *

    from {{ model }}

    where

        ({{ delay_column }} <= 0 and {{ status_column }} != 'on_time') or

        (
            {{ delay_column }} > 0
            and {{ delay_column }} <= 15
            and {{ status_column }} != 'minor_delay'
        )

        or

        ({{ delay_column }} > 15 and {{ status_column }} != 'delayed')

{% endtest %}
