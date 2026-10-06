import re
import json
import time
import math
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from tqdm.auto import tqdm
from src.utils.logging_utils import setup_logger


from src.etl.transformers.extraction_postprocess import (
    NO_RULE, deterministic_pct, postprocess, parse_response,
)

CHUNK_SIZE = 500


class DocumentFeaturesV2LLMTransformer:
    def __init__(self, schemas_path: str, llm_provider, few_shot_db_path: Optional[str] = None):
        self.llm = llm_provider
        self.logger = setup_logger(__name__)

        with Path(schemas_path).open(encoding="utf-8") as f:
            schemas = json.load(f)
        self.title_schemas = {k: v for k, v in schemas.items() if k != "meta"}
        self.field_schemas: Dict[str, dict] = {}
        self._field_to_title: Dict[str, str] = {}
        for slug, schema in self.title_schemas.items():
            for f in schema["fields"]:
                self.field_schemas[f["name"]] = f
                self._field_to_title[f["name"]] = slug

        self.few_shot_db: Dict = {}
        if few_shot_db_path and Path(few_shot_db_path).exists():
            with Path(few_shot_db_path).open(encoding="utf-8") as f:
                self.few_shot_db = json.load(f)
            total = sum(
                len(ex)
                for slug_data in self.few_shot_db.values()
                for cluster_data in slug_data.values()
                for ex_list in cluster_data.values()
                for ex in ex_list
            )
            self.logger.info(
                "Few-shot DB loaded: %d examples across %d titles",
                total, len(self.few_shot_db),
            )

    def _get_few_shot_examples(self, field_name: str, cluster_id) -> List[str]:
        slug = self._field_to_title.get(field_name)
        if not slug or not self.few_shot_db:
            return []
        try:
            cid_str = str(int(cluster_id))
        except (ValueError, TypeError):
            cid_str = None
        examples = []
        slug_data = self.few_shot_db.get(slug, {})
        if cid_str and cid_str in slug_data:
            cluster_examples = slug_data[cid_str].get(field_name, [])
            for ex in cluster_examples[:3]:
                text = ex.get("text", "")[:500]
                val = ex.get("value", "")
                examples.append(f"Ejemplo (cluster {cid_str}): valor={val}\n{text}")
        return examples

    def compute_llm_fallback(
        self, field_name: str, df_failed: pd.DataFrame,
        model_name: str = "unknown",
    ) -> Tuple[pd.DataFrame, dict]:
        schema = self.field_schemas.get(field_name)
        if not schema:
            self.logger.warning("No schema for field '%s'", field_name)
            return pd.DataFrame(), {}

        ftype = schema["type"]

        use_system_role = "mistral" not in model_name.lower()

        all_messages = []
        valid_indices = []
        for idx, row in df_failed.iterrows():
            text = row.get("content_text") or ""
            if not text.strip():
                continue
            cluster_id = row.get("cluster_id")
            if use_system_role:
                all_messages.append(self._build_messages(schema, text, cluster_id))
            else:
                all_messages.append(self._build_messages_without_system(schema, text, cluster_id))
            valid_indices.append(idx)

        if not all_messages:
            self.logger.warning("No texts to extract for %s.", field_name)
            return pd.DataFrame(), {}

        n_total = len(all_messages)
        n_chunks = math.ceil(n_total / CHUNK_SIZE)
        t_start = time.time()
        all_results = []
        total_tok_in = 0
        total_tok_out = 0
        total_ok = 0

        pbar = tqdm(
            total=n_total, desc=f"  {field_name[:30]:30s}", unit="doc",
            bar_format="{desc} [{n_fmt}/{total_fmt} |{bar}| {rate_fmt} {postfix}",
        )

        for chunk_i in range(n_chunks):
            lo = chunk_i * CHUNK_SIZE
            hi = min(lo + CHUNK_SIZE, n_total)
            chunk_msgs = all_messages[lo:hi]
            chunk_idxs = valid_indices[lo:hi]

            t_chunk = time.time()
            responses = self.llm.generate_batch_messages(chunk_msgs, max_tokens=256)
            elapsed = time.time() - t_chunk

            for idx, resp in zip(chunk_idxs, responses):
                tok_out = len(resp) // 4
                total_tok_out += tok_out
                raw_txt = resp.strip()
                raw = raw_txt.upper()
                text = df_failed.at[idx, "content_text"] or ""
                value, conf = self._parse_response(raw_txt, ftype, field=field_name, text=text)
                all_results.append({
                    "nro_licitacion": df_failed.at[idx, "nro_licitacion"],
                    "field_name": field_name,
                    "llm_value": value,
                    "llm_confidence": conf,
                    "llm_model": model_name,
                    "llm_raw": raw_txt[:120],
                })
                if conf > 0:
                    total_ok += 1

            for msg in chunk_msgs:
                total_tok_in += len(msg[1]["content"]) // 4

            done = min(hi, n_total)
            rate = CHUNK_SIZE / elapsed if elapsed > 0 else 0
            eta_s = (n_total - done) / rate if rate > 0 else 0
            pbar.set_postfix({
                "ok": f"{total_ok}/{done}",
                "tok/s": f"{total_tok_out/max(time.time()-t_start, 0.1):.0f}",
                "eta": f"{eta_s/60:.0f}m" if eta_s > 60 else f"{eta_s:.0f}s",
            })
            pbar.update(hi - lo)

        pbar.close()
        elapsed_total = time.time() - t_start

        df = pd.DataFrame(all_results)
        n_found = int(df["llm_confidence"].sum())

        metrics = {
            "field": field_name,
            "docs": n_total,
            "found": n_found,
            "pct": round(n_found / n_total * 100, 1) if n_total else 0,
            "tok_in": total_tok_in,
            "tok_out": total_tok_out,
            "time_s": round(elapsed_total, 1),
            "docs_per_s": round(n_total / elapsed_total, 2) if elapsed_total else 0,
        }

        self.logger.info(
            "  %s: %d/%d (%.0f%%) | %d tok in, %d tok out | %.1fs (%.1f docs/s)",
            field_name, n_found, n_total, metrics["pct"],
            total_tok_in, total_tok_out, elapsed_total, metrics["docs_per_s"],
        )
        return df, metrics

    def _build_messages(self, schema: dict, text: str, cluster_id=None) -> List[Dict[str, str]]:
        system = self._build_system_prompt()
        field_name = schema["name"]
        hint = schema.get("extraction_hint", "")
        user_parts = [
            f"Campo a extraer: {schema['description']}",
            f"Tipo esperado: {schema['type']}",
        ]
        if hint and hint not in ("", "None"):
            user_parts.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
        few_shot = self._get_few_shot_examples(field_name, cluster_id)
        if few_shot:
            user_parts.append("\nEjemplos de documentos similares:")
            for ex in few_shot:
                user_parts.append(f"---\n{ex}")
        user_parts.append(f"\nTexto del pliego:\n{text}")
        return [
            {"role": "system", "content": system},
            {"role": "user", "content": "\n".join(user_parts)},
        ]

    def _build_messages_without_system(self, schema: dict, text: str, cluster_id=None) -> List[Dict[str, str]]:
        system = self._build_system_prompt()
        field_name = schema["name"]
        hint = schema.get("extraction_hint", "")
        user_parts = [
            system,
            f"Campo a extraer: {schema['description']}",
            f"Tipo esperado: {schema['type']}",
        ]
        if hint and hint not in ("", "None"):
            user_parts.append(f"Gu\u00eda de extracci\u00f3n: {hint}")
        few_shot = self._get_few_shot_examples(field_name, cluster_id)
        if few_shot:
            user_parts.append("\nEjemplos de documentos similares:")
            for ex in few_shot:
                user_parts.append(f"---\n{ex}")
        user_parts.append(f"\nTexto del pliego:\n{text}")
        return [
            {"role": "user", "content": "\n".join(user_parts)},
        ]

    @staticmethod
    def _build_system_prompt() -> str:
        return (
            "Eres un extractor de datos de pliegos de licitaciones p\u00fablicas paraguayas (DNCP).\n"
            "Extrae el valor del campo solicitado siguiendo exactamente la gu\u00eda de extracci\u00f3n.\n"
            "\n"
            "REGLAS ESTRICTAS:\n"
            "- No confundas tipos: si el campo pide d\u00edas, no extraigas porcentajes (10%) ni montos.\n"
            "- Verifica que el n\u00famero est\u00e9 asociado a la unidad correcta (d\u00edas, %, a\u00f1os).\n"
            "- Los porcentajes tienen el s\u00edmbolo '%' o texto 'por ciento' junto al n\u00famero.\n"
            "- No extraigas n\u00fameros de cl\u00e1usulas, art\u00edculos legales, ni listas numeradas.\n"
            "- Si el \u00fanico n\u00famero en el texto corresponde a otro concepto (otro campo), responde NULL.\n"
            "- La gu\u00eda de extracci\u00f3n tiene prioridad sobre estas reglas generales.\n"
            "\n"
            "FORMATO DE RESPUESTA:\n"
            "- Para int/float: responde SOLO con el n\u00famero (usa punto decimal, sin unidades).\n"
            "- Para binary: responde SOLO con 1 (si se cumple) o 0 (si no).\n"
            "- Para categorical: responde SOLO con la categor\u00eda exacta.\n"
            "- Si no encuentras el valor o no es v\u00e1lido, responde exactamente: NULL"
        )

    @staticmethod
    def _parse_response(raw: str, ftype: str, field: str = "", text: str = ""):
        """Parse a raw LLM response with the deterministic postprocess layer.

        Deterministic fields (uniform template pct_*) are resolved from the
        text without consulting the model output. For the rest, the strict
        postprocess rules (x100 scale, context validation) are applied after
        extracting the number from the raw response.
        """
        det = deterministic_pct(field, text)
        if det is not NO_RULE:
            return det, 1.0

        if raw in ("", "NULL", "NONE", "N/A", "NA", "NOT FOUND"):
            return None, 0.0

        cleaned = re.sub(r" thinking.*? response", "", raw, flags=re.DOTALL).strip()
        if not cleaned:
            cleaned = raw

        m = re.search(r"-?\d+[.,]?\d*", cleaned.replace(" ", ""))
        if not m:
            return None, 0.0

        try:
            val_str = m.group(0).replace(",", ".")
            if ftype == "int":
                val = int(float(val_str))
            else:
                val = float(val_str)
        except (ValueError, TypeError):
            return None, 0.0

        val = postprocess(field, val, text, strict=True)
        if val is None:
            return None, 0.0
        return val, 1.0