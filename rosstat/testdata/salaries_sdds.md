# Salary SDDS fixture

Minimal generated workbook using salary facts and layout anchors from Rosstat, retrieved 2026-11-15:
https://rosstat.gov.ru/storage/mediabank/SDDS_labor_market.xlsx

Original source SHA256: 70b53e0e07399f0ef9c0b2cf536a3881dbf242f4b5729690de57ed7b69107a1c (21986 bytes).
The full source workbook is not redistributed. The fixture contains only numeric
date headers, the salary row (needed for the full overlap assertion), and two
synthetic neighboring labour rows with deliberately artificial values of 999.
Styles, metadata, footnotes, and actual labour observations are excluded. Sheet «Рынок труда», A6 salary,
B6 «рубли», C6–EL6: Jan 2015–Aug 2026 (140 observations). EM1 September header
has no corresponding salary value in EM6. Existing primary fixture overlaps 139
months; live research #604 independently checked 140/140 against dev history.
