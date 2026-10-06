"""Tests del postproceso (plan de corrección F2; auditoría H1.1, H1.2, H2.1, H4.2, H4.3).

Correr: /root/vllm-env/bin/python -m pytest tests/ -q
"""
import json
import math
import re
import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from corpus import B, GT_DIR, iter_corpus  # noqa: E402
from src.etl.transformers import extraction_postprocess as ep  # noqa: E402
from src.etl.transformers.document_features_v2_llm_transformer import (  # noqa: E402
    DocumentFeaturesV2LLMTransformer)

SNAPSHOT = ROOT / "tests/fixtures/legacy_snapshot.json"
DIAS = ["dias_validez_garantia", "anos_experiencia_minima_tecnica",
        "dias_pago_anticipo", "dias_solicitud_anticipo"]


def _f(v):
    return None if v is None else float(v)


@pytest.fixture(scope="module")
def corpus():
    return list(iter_corpus())


@pytest.fixture(scope="module")
def snapshot():
    return json.loads(SNAPSHOT.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def c3_items():
    from corpus import gt_text_index
    gti = gt_text_index()
    rs = json.loads((B / "C3_Llama_31_8B-FP8.json").read_text(encoding="utf-8"))["results"]
    return [(r["field"], r["gt"], gti[(r["nro"], r["field"])]) for r in rs]


def _eq(a, b, field):
    if a is None or b is None:
        return a is None and b is None
    if abs(a - b) < 1e-9:
        return True
    return field.startswith("pct_") and (abs(a * 100 - b) < 1e-6 or abs(a - b * 100) < 1e-6)


# ------------------------------------------------ histórico congelado (F2.0)
def test_snapshot_covers_corpus(corpus, snapshot):
    assert len(corpus) == len(snapshot) == 40950
    assert {c[0] for c in corpus} == set(snapshot)


def test_legacy_benchmark_mode_reproduces_snapshot(corpus, snapshot):
    bad = [cid for cid, f, raw, text, _ in corpus
           if _f(ep.resolve(f, raw, text, mode="rule_then_llm").value) != snapshot[cid][0]]
    assert not bad, f"{len(bad)} difieren, p.ej. {bad[:3]}"


def test_legacy_production_mode_reproduces_snapshot(corpus, snapshot):
    bad = []
    for cid, f, raw, text, ftype in corpus:
        v, _ = DocumentFeaturesV2LLMTransformer._parse_response(
            raw.strip(), ftype, field=f, text=text, mode="llm_gated_rule")
        if _f(v) != snapshot[cid][1]:
            bad.append(cid)
    assert not bad, f"{len(bad)} difieren, p.ej. {bad[:3]}"


def test_public_aliases_stay_historical():
    assert ep.parse_response is ep._legacy_parse_response
    assert ep.postprocess is ep._legacy_postprocess


# ------------------------------------------------ hallazgo central (H2.1)
def test_always_null_scores_239_only_with_rules(c3_items):
    dias = [(f, gt, t) for f, gt, t in c3_items if f in DIAS]
    assert len(dias) == 239

    def hits(mode):
        return sum(_eq(_f(ep.resolve(f, "NULL", t, mode=mode).value), gt, f)
                   for f, gt, t in dias)
    assert hits("rule_then_llm") == 239   # lo publicado: las reglas responden
    assert hits("llm_only") == 0          # midiendo al modelo, NULL no acierta nada
    assert hits("rule_first") == 239      # producción: las reglas siguen decidiendo, con source='rule'
    srcs = {ep.resolve(f, "NULL", t, mode="rule_first").source for f, _, t in dias}
    assert srcs == {"rule"}


# ------------------------------------------------ reglas sin regresión
def test_rule_value_matches_legacy_rules_except_intended(corpus):
    """rule_value == lo que el parser histórico devolvía con raw=NULL, salvo la
    corrección intencional de dias_aceptacion_rechazo_factura (H4.2)."""
    diffs = {}
    for cid, f, raw, text, _ in corpus:
        if raw != "NULL" or (f not in ep.RULES and ep.deterministic_pct(f, text) is ep.NO_RULE):
            continue
        legacy = _f(ep._legacy_parse_response("NULL", field=f, text=text))
        rv = ep.rule_value(f, text)
        new = None if rv is ep.NO_RULE else _f(rv)
        if legacy != new:
            diffs.setdefault(f, []).append(cid)
    assert set(diffs) <= {"dias_aceptacion_rechazo_factura"}, {k: len(v) for k, v in diffs.items()}


def test_aceptacion_rule_not_worse_than_legacy_on_gt():
    g = pd.read_csv(GT_DIR / "dias_aceptacion_rechazo_factura_GT.csv")
    old = new = 0
    for r in g.itertuples():
        gt = None if pd.isna(r.ground_truth) else float(r.ground_truth)
        text = str(r.text)
        old += _eq(_f(ep._legacy_parse_response("NULL", field=r.campo, text=text)), gt, r.campo)
        rv = ep.rule_value(r.campo, text)
        new += _eq(None if rv is ep.NO_RULE else _f(rv), gt, r.campo)
    assert new >= old, (new, old)


@pytest.mark.parametrize("text,expected", [
    ("La factura será aceptada o rechazada dentro de los 15 días", 15.0),
    ("dentro de los quince (15) días corridos", 15.0),
    ("en un plazo de 15 (quince) días hábiles", 15.0),
    ("dentro de quince días", 15.0),
    ("Resolución DNCP 2015, monto 150 millones", None),
    ("retención del 15% del monto", None),
    ("plazo de 150 días", None),
    ("", None),
])
def test_aceptacion_15_rule_edges(text, expected):
    assert ep.rule_value("dias_aceptacion_rechazo_factura", text) == expected


# ------------------------------------------------ llm_value nunca sustituye (H1.1, H4.3)
def test_llm_value_never_substitutes_model_number(corpus):
    raw_re = re.compile(r"^-?\d+(?:[.,]\d+)?$")
    bad = []
    for cid, f, raw, text, ftype in corpus:
        v = ep.llm_value(f, raw, text, ftype)
        if v is None:
            continue
        s = raw.strip()
        assert raw_re.match(s), cid
        r = float(s.replace(",", "."))
        if not (math.isclose(v, r) or (f in ep.PCT_INT_SCALE and math.isclose(v, r * 100))):
            bad.append((cid, raw, v))
    assert not bad, bad[:5]


@pytest.mark.parametrize("raw", ["1.5", "1.1", "2,5"])
def test_int_fields_reject_decimal_instead_of_truncating(raw):
    text = "Documentos: 1. Factura 1.5 Nota de remisión 2 copias 1 1.1 2,5"
    assert ep.llm_value("num_documentos_pago", raw, text, ftype="int") is None


def test_int_fields_accept_integers():
    assert ep.llm_value("num_documentos_pago", "3", "presentar 3 documentos", ftype="int") == 3.0


# ------------------------------------------------ resolve / modos
def test_rule_first_source_is_rule_whenever_rule_applies(corpus):
    for cid, f, raw, text, ftype in corpus[:5000]:
        r = ep.resolve(f, raw, text, mode="rule_first", ftype=ftype)
        if r.rule_applies:
            assert r.source == "rule" and r.value == r.rule_value
        else:
            assert r.source in ("llm", "none") and r.value == r.llm_value


def test_rule_only_ignores_model_output(corpus):
    for cid, f, raw, text, ftype in corpus[:3000]:
        a = ep.resolve(f, raw, text, mode="rule_only", ftype=ftype)
        b = ep.resolve(f, "NULL", text, mode="rule_only", ftype=ftype)
        assert a.value == b.value and a.source == b.source


def test_unknown_mode_raises():
    with pytest.raises(ValueError):
        ep.resolve("dias_pago_maximo", "10", "10 días", mode="magic")


def test_production_default_mode():
    assert ep.PRODUCTION_MODE == "rule_first"
    t = DocumentFeaturesV2LLMTransformer.__init__.__defaults__
    assert "rule_first" in t


# ------------------------------------------------ regex de producción num_documentos_pago (H4.3)
@pytest.fixture(scope="module")
def regex_transformer():
    from src.etl.transformers.document_features_v2_transformer import (
        DocumentFeaturesV2Transformer)
    return DocumentFeaturesV2Transformer(
        str(ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"))


def test_num_documentos_pago_counts_named_documents_on_gt(regex_transformer):
    g = pd.read_csv(GT_DIR / "num_documentos_pago_GT.csv")
    ok = sum((regex_transformer._re_num_documentos_pago(str(r.text)) or None)
             == (None if pd.isna(r.ground_truth) else int(r.ground_truth))
             for r in g.itertuples())
    assert ok == 200  # in-sample (receta derivada de la nota del GT): validar en F5


def test_num_documentos_pago_is_not_first_item_index(regex_transformer):
    text = ("acompañada de los siguientes documentos:\n1. Nota de remisión\n"
            "2. La factura de pago\n3. REPSE\nOtras formas y condiciones de pago")
    assert regex_transformer._re_num_documentos_pago(text) == 3
    assert regex_transformer._re_num_documentos_pago("sin lista de documentos") is None
