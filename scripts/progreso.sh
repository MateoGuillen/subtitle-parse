#!/usr/bin/env bash
# Progreso en vivo del plan de corrección (sesión 23+). Uso en otra terminal WSL:
#   watch -n 10 bash /mnt/datos/projects/subtitle-parse/scripts/progreso.sh
cd "$(dirname "$0")/.." || exit 1
RUN=${RUN:-$(podman exec dncp psql -U postgres -d dncp -At -c \
  "SELECT run_id FROM dncp.llm_runs ORDER BY started_at DESC LIMIT 1")}
echo "== $(date '+%H:%M:%S')  rama $(git branch --show-current)  HEAD $(git log --oneline -1 | cut -c1-70)"
echo "== Procesos"
pgrep -af "run_llm_fallback|ablacion_gt|generar_aceptacion|varianza_campos" | cut -c1-150 || echo "  (ninguno corriendo)"
echo "== GPU"
nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total --format=csv,noheader
echo "== Run $RUN"
podman exec dncp psql -U postgres -d dncp -At -F ' | ' -c "
SELECT status, count(*), round(100.0*count(*)/sum(count(*)) OVER (),1)||'%'
FROM dncp.llm_fallback_runs_v2 WHERE run_id='$RUN' GROUP BY 1 ORDER BY 1;"
echo "-- pendientes por campo"
podman exec dncp psql -U postgres -d dncp -At -F ' | ' -c "
SELECT field, count(*) FILTER (WHERE status='pending') pend,
       count(*) FILTER (WHERE status='done' AND raw IS NOT NULL) llm_ok
FROM dncp.llm_fallback_runs_v2 WHERE run_id='$RUN' GROUP BY 1
HAVING count(*) FILTER (WHERE status='pending') > 0 ORDER BY 2 DESC;"
echo "== Última línea del log"
tail -c 300 $(ls -t /root/backups/*.log | head -1) 2>/dev/null | tr '\r' '\n' | grep -v '^$' | tail -1 | cut -c1-120
echo "== Ronda 2 de candidatos (descarga / test)"
for m in gemma-4-12b-qat-w4a16 qwen3.5-9b-fp8 ministral-3-8b-gguf ling-3.0-tiny-gguf mimo-v2.6-9b-gguf clef-flash-gguf gemma-4-26b-a4b-gguf qwen3.6-35b-a3b-gguf; do
  d="bajando/en cola"; [ -f /root/models/gguf/.ok_$m ] && d="descargado"; [ -f /root/models/gguf/.fail_$m ] && d="DESCARGA FALLÓ"
  L=/root/backups/retest_$m.log; t=""
  [ -f "$L" ] && t=$(grep -h "exit=\|FALLÓ" "$L" || tr '\r' '\n' < "$L" | grep -E '^\s+[a-z_]+: [0-9]+ en' | tail -1)
  printf "  %-24s %-16s %s\n" "$m" "$d" "${t:-—}"
done
