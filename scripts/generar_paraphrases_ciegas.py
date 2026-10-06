"""Genera P3 y P4 CIEGAS: Mistral-7B (nunca expuesto a resultados de C4) reescribe el
system prompt de extraccion a partir SOLO de una lista de funcionalidades abstractas F.

Protocolo anti-fuga (pre-registrado):
 - F describe QUE debe hacer el prompt, nunca un orden de secciones, frases o ejemplos
   tomados del system baseline ni de P2.
 - Antes de lanzar la generacion se verifican y registran en ciegas_check.json:
   (a) F no contiene cadenas distintivas de P2 (>20 chars compartidas),
   (b) Mistral no ve los prompts base/P2 ni ningun resultado.
 - Temperatura 1.0, dos muestras independientes = dos reescrituras de origen unico ciego.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

F = """Un sistema debe extraer informacion numerica de documentos legales de contratacion publica.
El system prompt debe lograr estas cosas, ninguna otra:
1. Definir el rol del modelo como lector-analista de fragmentos de pliegos.
2. Exigir que la unica fuente sea el fragmento provisto, prohibiendo inventar o usar conocimiento externo.
3. Exigir literalidad: el valor sale del texto tal cual esta escrito ahi.
4. Definir que si el dato pedido no esta, la respuesta correcta es NULL; la ausencia no se rellena con valores cercanos.
5. Fijar el contrato de salida: exactamente un numero o la palabra NULL.
6. Fijar que el valor va en la primera linea, permitiendo explicacion debajo si el modelo la quisiera.
7. Prohibir copiar numeros que no sean el dato pedido (articulos, leyes, anos, porcentajes de otras clausulas).
No agregues funciones que no esten listadas."""

# (a) verificacion de fuga F vs P2 (cadenas largas compartidas); P2 real importado, no retipeado
import importlib.util
_spec = importlib.util.spec_from_file_location(
    'c4', str(ROOT / 'scripts/c4_paraphrase.py'))
_c4 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_c4)
P2 = _c4.SYSTEM_P2
shared = [P2[i:i + 25] for i in range(len(P2) - 25) if P2[i:i + 25] in F]
print('fugas F~P2 (25 chars):', shared or 'ninguna')
assert not shared

from vllm import LLM, SamplingParams
llm = LLM(model='solidrust/Mistral-7B-Instruct-v0.3-AWQ', max_model_len=4096,
          gpu_memory_utilization=0.70, enforce_eager=True, dtype='bfloat16',
          quantization=None)
user = ('Reescribe EN ESPAÑOL un system prompt que cumpla exactamente estas '
        'funcionalidades, con Tu propio estilo y orden (que no tiene que ser el de la '
        'lista). NO agregues ni quetes funciones. Maximo 120 palabras.\n\n'
        + F)
msgs = [{'role': 'user', 'content': user}]
sp = SamplingParams(max_tokens=220, temperature=1.0)
t0 = time.time()
# dos muestras independientes = dos reescrituras de origen unico ciego
outs = llm.chat([msgs, msgs], sp, use_tqdm=False)
texts = [o.outputs[0].text.strip() for o in outs]
out = {'P3': texts[0], 'P4': texts[1], 'F': F, 'generator': 'solidrust/Mistral-7B v0.3 AWQ temp1.0',
       'fuga_F_vs_P2': shared, 'elapsed': time.time() - t0}
(ROOT / 'data/processed/llm_benchmark/paraphrases_ciegas.json').write_text(
    json.dumps(out, ensure_ascii=False, indent=1))
print('P3:', texts[0][:180].replace('\n', ' '))
print('P4:', texts[1][:180].replace('\n', ' '))
del llm
print('OK', time.time() - t0)
