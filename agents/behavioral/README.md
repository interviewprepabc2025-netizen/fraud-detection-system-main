# agents/behavioral/

## Responsibility

Detects deviations from a user's established spending and transactional behaviour.

## Signals evaluated

- Amount deviation from 30-day rolling average
- Unusual merchant category code (MCC) for this user
- Unusual time-of-day for this user's spending patterns
- First-time merchant interaction with a large amount

## Consumer group

`behavioral-group`
