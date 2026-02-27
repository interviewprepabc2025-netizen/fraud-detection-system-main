# agents/geolocation/

## Responsibility

Detects impossible travel, high-risk geographies, and location-based anomalies.

## Signals evaluated

- Distance between current and previous transaction location (km)
- Time elapsed vs. distance (impossible travel speed)
- New country or city for the user
- High-risk country codes
- IP geolocation mismatch with card billing country

## Consumer group

`geolocation-group`
