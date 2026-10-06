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
tail -c 300 /root/backups/run_f4_full.log 2>/dev/null | tr '\r' '\n' | grep -v '^$' | tail -1 | cut -c1-120
