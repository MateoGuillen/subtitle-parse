"""Tests de la lógica de validación de planillas v2 (plan F5; auditoría H3.3, H3.4)."""
import importlib.util
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("va", ROOT / "scripts/validar_aceptacion.py")
va = importlib.util.module_from_spec(spec)
sys.modules["va"] = va
spec.loader.exec_module(va)


def row(campo="dias_pago_maximo", aud="", estado="", sist=None):
    return {"id": "x", "campo": campo, "valor_auditor": aud, "estado": estado,
            "valor_sistema": sist}


@pytest.mark.parametrize("r,expected", [
    (row(aud="60", sist=60.0), True),
    (row(aud="30", sist=60.0), False),
    (row(estado="no_fija", sist=None), True),     # sistema dijo NULL y el pliego no fija
    (row(estado="no_fija", sist=60.0), False),    # sistema inventó un valor
    (row(aud="60", sist=None), False),            # sistema omitió un valor real
    (row(estado="ambiguo", sist=60.0), None),     # ambiguo: aparte (cuenta como error)
    (row(campo="pct_monto_referencia_tecnica", aud="50", sist=0.5), True),  # escala %
])
def test_veredicto(r, expected):
    assert va.veredicto(r) is expected


def test_veredicto_requires_value_or_state():
    with pytest.raises(ValueError):
        va.veredicto(row(sist=60.0))


def test_promote_threshold_reachable_with_n70():
    """Con n=70 plantillas, 66/70 alcanza LCB>=0.85 y 65/70 no (justifica n=70)."""
    assert va.wilson(66, 70)[0] >= 0.85
    assert va.wilson(65, 70)[0] < 0.85


def test_v1_tanda_c_could_never_promote():
    """H3.3: con n=13 por campo, ni 13/13 alcanza LCB 0.85."""
    assert va.wilson(13, 13)[0] < 0.85


def test_kappa_perfect_and_chance():
    a = pd.DataFrame({"estado": ["", "", "no_fija", ""], "valor_auditor": ["1", "2", "", "1"]})
    po, k = va.kappa(a, a.copy())
    assert po == 1.0 and k == pytest.approx(1.0)
    b = pd.DataFrame({"estado": ["", "", "", "no_fija"], "valor_auditor": ["2", "1", "1", ""]})
    po2, k2 = va.kappa(a, b)
    assert po2 < 1.0 and k2 < 0.5
