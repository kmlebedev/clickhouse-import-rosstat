"""Offline arithmetic checks of recorded operands; does not re-fetch source books."""
import json
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def read(name):
    return json.loads((ROOT / name).read_text())


checks = []


def check(name, actual, expected):
    if actual != expected:
        raise ValueError(f'{name}: {actual} != {expected}')
    checks.append(name)


controls = read('population-evidence.json')['census_490']['controls']
check('census national sex sum', sum(controls[0]['operands']), controls[0]['published_total'])
check('census national books parity', *controls[2]['values'])
ages = controls[3]['result_by_sex_persons']
check('census fixed 15–64 sex sum', ages['C'] + ages['D'], ages['B'])
for row in read('hours-evidence.json')['national_total_hours_checks']:
    residual = Decimal(row['total']['value']) - Decimal(row['hired']['value']) - Decimal(row['non_hired']['value'])
    check(f"hours {row['year']} recomputed residual", residual, Decimal(row['residual']))
    check(f"hours {row['year']} rounding bound", abs(residual) <= Decimal(row['rounding_bound']), True)
for name in ('catalog', 'labour'):
    check(f'MCP {name} unchanged', read('baseline.json')[name], read('after.json')[name])
print(json.dumps({'passed': len(checks), 'checks': checks, 'scope': 'Recorded operands only; no independent XLS replay or validation of all 132 distribution groups'}, ensure_ascii=False, indent=2))
