# agents/network/

## Responsibility

Analyses network-level signals: IP reputation, device fingerprinting, and
account linkage graphs to surface linked fraud rings.

## Signals evaluated

- IP fraud score from online feature store
- Whether device fingerprint has been seen before
- Number of accounts linked to this device or IP
- VPN / proxy / Tor exit node detection
- Known-bad device fingerprint list

## Consumer group

`network-group`
