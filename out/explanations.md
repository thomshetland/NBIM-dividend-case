# Explanations

## 960789012  
*ISIN:* `KR7005930003`  
The dividend reconciliation for Samsung Electronics reveals material variances in tax calculation and foreign exchange conversion. The primary drivers are the 2% tax rate difference and substantial FX rate inconsistency, leading to a $450,050 tax quote delta. Custody's USD settlement amount appears anomalous, suggesting potential computational or data transmission issues.

- Tax rate differs: NBIM at 22% vs Custody at 20%, causing $450,050 tax delta
- FX conversion rates significantly diverge: NBIM uses 0.008234 vs Custody at 1307.25
- Custody net amount in USD shows settlement discrepancy at $5,524.27

## 950123456  
*ISIN:* `US0378331005`  
The dividend reconciliation for Apple Inc shows precise alignment between NBIM and Custody systems across key financial metrics. Both sources confirm a $0.25 per share dividend on 1,500,000 shares, resulting in a gross dividend of $375,000, with a 15% tax rate generating $56,250 in withholding tax. The net dividend of $318,750 is consistent across systems, suggesting a clean, error-free dividend processing event.

- NBIM and Custody records match exactly for gross dividend ($375,000), tax amount ($56,250), and net dividend ($318,750)
- FX rates differ: NBIM shows 11.2345, Custody shows 1.0, but no material impact on settlement
- No ADR fees detected for this Apple dividend event

## 970456789  
*ISIN:* `CH0038863350`  
The dividend reconciliation for Nestle SA reveals minor discrepancies primarily driven by position size differences. The custody source reports a larger nominal position of 60,000 versus NBIM's 45,000, resulting in a gross dividend delta of CHF 6,200. Both sources apply a consistent 35% tax rate, though computational nuances create a small tax variance of CHF 2,170. The most significant methodological difference appears in foreign exchange treatment, with NBIM using a complex FX rate of 12.4567 compared to custody's flat 1.0 rate.

- Custody position (60,000) differs from NBIM position (45,000), causing gross dividend variance of CHF 6,200
- Tax calculation consistent at 35% across sources, with slight computational differences
- FX rate divergence noted: NBIM uses 12.4567, Custody uses 1.0

