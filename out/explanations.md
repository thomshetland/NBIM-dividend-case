# Explanations

## 960789012  
*ISIN:* `KR7005930003`  
The dividend reconciliation for Samsung Electronics reveals discrepancies primarily driven by tax rate interpretation and foreign exchange calculations. The NBIM system calculates a higher tax amount due to a 22% tax rate, while the Custody system uses a 20% rate. Additionally, the extreme variance in FX rates suggests a potential conversion error, which significantly impacts the net settlement amount. The derived delta flags an FX mismatch, indicating these systems require further alignment and validation of their dividend processing parameters.

- Tax rate differs: NBIM at 22% vs. Custody at 20%
- FX conversion rates are significantly different: NBIM at 0.008234 vs. Custody at 1307.25
- Net amount in quote currency shows a $450,050 variance between NBIM and Custody systems

## 950123456  
*ISIN:* `US0378331005`  
The dividend event for Apple Inc (AAPL) shows consistent key details across NBIM and Custody systems. Both sources confirm a $0.25 dividend per share on 1,500,000 shares, resulting in a gross dividend of $375,000 and a tax amount of $56,250. The primary reconciliation point of interest is the foreign exchange (FX) rate, with NBIM reporting a significantly different rate compared to the Custody system, which could impact settle currency calculations.

- Gross dividend amount matches between NBIM and Custody at $375,000
- Tax amount is consistent at $56,250 (15% tax rate)
- FX rate differs: NBIM shows 11.2345 vs Custody at 1.0

## 970456789  
*ISIN:* `CH0038863350`  
The dividend reconciliation for Nestle SA reveals discrepancies in position size and gross dividend calculation. The primary differences stem from varying share counts and potential FX interpretation, with NBIM and Custody systems reporting different nominal positions. The tax rate remains consistent at 35% for both sources, but the gross and net dividend amounts show a delta of CHF 6,200 in gross and CHF 4,030 in net dividend.

- Position mismatch: NBIM shows 45,000 shares, Custody shows 60,000 shares
- Gross dividend amount differs: NBIM reports CHF 139,500 vs Custody CHF 145,700
- FX rates significantly different: NBIM uses 12.4567, Custody uses 1.0

