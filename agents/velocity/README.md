# agents/velocity/

## Responsibility

Detects rapid-fire transaction patterns that indicate card testing, account takeover,
or automated fraud attacks.

## Signals evaluated

- Number of transactions in the last 1 h / 24 h for a user
- Total spend in the last 1 h / 24 h
- Transactions per merchant in the last 1 h
- Burst rate (transactions per minute in a 5-minute sliding window)

## Risk score logic

| Condition | Score contribution |
|---|---|
| > 10 txns/hour | +0.30 |
| > 3 x avg hourly spend | +0.25 |
| > 5 txns to same merchant/hour | +0.20 |
| Burst rate > 5 txns/min | +0.25 |

## Consumer group

`velocity-group`
