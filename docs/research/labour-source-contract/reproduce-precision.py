#!/usr/bin/env python3
"""Offline #608 evidence arithmetic: read repository JSON, assert, print summary."""
import collections
import json
from decimal import Decimal as D, ROUND_HALF_UP
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3] / 'docs/research/macro-source-recovery'


def read(name):
    return json.loads((ROOT / name).read_text(), parse_float=D, parse_int=D)


sdds = read('labour-sdds-overlap.json')
emiss = read('labour-emiss-overlap.json')
baseline = collections.defaultdict(dict)
for series, date, value in read('baseline.json')['queries']['data']['result']['rows']:
    baseline[date][series] = value
F, E, U = 'Рабочая сила', 'Занятые', 'Безработные'
P, R, V = 'Уровень участия в составе рабочей силы', 'Уровень занятости', 'Уровень безработицы'
h, rate_h = D('.0005'), D('.05')
coarse = {(p['series'], p['date']) for p in sdds['discrepancies']}
assert len(coarse) == 32
for p in sdds['discrepancies']:
    raw = D(p['raw_million']) * 1000
    rounded = raw.quantize(D('1E2'))
    assert abs(raw - rounded) < D('1e-8')
    assert abs(rounded - p['baseline_thousand']) < D('50.0005')

sd = collections.defaultdict(dict)
for p in sdds['points']:
    sd[p['date']][p['series']] = D(p['raw_million']) * 1000
for date, x in sd.items():
    bound = sum(D(50) if (n, date) in coarse else h for n in (F, E, U))
    assert abs(x[F] - x[E] - x[U]) <= bound
complete = 0
max_sum_error = D(0)
for x in baseline.values():
    if not all(n in x for n in (F, E, U, P, R, V)):
        continue
    complete += 1
    residual = abs(x[F] - x[E] - x[U])
    max_sum_error = max(residual, max_sum_error)
    assert residual <= 3 * h
    u_lo, u_hi = 100 * (x[U] - h) / (x[F] + h), 100 * (x[U] + h) / (x[F] - h)
    assert u_lo <= x[V] + rate_h and u_hi >= x[V] - rate_h
    ratio_lo, ratio_hi = (x[R] - rate_h) / (x[P] + rate_h), (x[R] + rate_h) / (x[P] - rate_h)
    assert ratio_lo <= (x[E] + h) / (x[F] - h)
    assert ratio_hi >= (x[E] - h) / (x[F] + h)
assert complete == 116

ties = []
for series in emiss:
    for p in series['points']:
        if series['series'] == P and abs(p['value'] - p['baseline']) == rate_h:
            assert p['value'].quantize(D('.1'), rounding=ROUND_HALF_UP) != p['baseline']
            latent = p['value'] - D('.001')
            assert latent.quantize(D('.01'), rounding=ROUND_HALF_UP) == p['value']
            assert latent.quantize(D('.1'), rounding=ROUND_HALF_UP) == p['baseline']
            ties.append(p['date'])
assert set(ties) == {'2023-08-31', '2024-05-31'}

sept = sd['2026-09-30']
u = 100 * sept[U] / sept[F]
u_interval = [100 * (sept[U] - h) / (sept[F] + h), 100 * (sept[U] + h) / (sept[F] - h)]
assert D('2.15') < u_interval[0] < u_interval[1] < D('2.25')
p_lo = D('61.75') * (sept[F] - h) / (sept[E] + h)
p_hi = D('61.85') * (sept[F] + h) / (sept[E] - h)
counterexamples = []
for latent in (D('63.12'), D('63.18')):
    implied_e = latent * sept[E] / sept[F]
    assert p_lo < latent < p_hi
    assert implied_e.quantize(D('.1'), rounding=ROUND_HALF_UP) == D('61.8')
    counterexamples.append({'latent_p': latent, 'implied_e': implied_e, 'rounded_p': latent.quantize(D('.1'), rounding=ROUND_HALF_UP)})
assert {p['rounded_p'] for p in counterexamples} == {D('63.1'), D('63.2')}
print(json.dumps({'baseline_months': complete, 'SDDS_months': len(sd), 'coarse_cells': len(coarse), 'half_tie_dates': ties, 'baseline_max_sum_error_thousand': max_sum_error, 'September_derived_u_percent': u, 'September_conditional_u_interval': u_interval, 'September_conditional_p_interval': [p_lo, p_hi], 'p_counterexamples': counterexamples, 'official_monthly_p': 'not established; PDF official e/u inputs are parent-verified, not re-fetched by this helper'}, default=str, ensure_ascii=False, indent=2))
