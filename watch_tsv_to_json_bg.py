#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
watch_tsv_to_json_bg.py (versão reforçada)
- Cria pastas no início (antes de importar watchdog)
- Loga em monitor.log
- Modo clique (sem parâmetros): entrada/saida/processados ao lado do script
"""

import csv
import json
import sys
import time
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict

BASE_DIR = Path(__file__).parent.resolve()
LOG_PATH = BASE_DIR / "monitor.log"

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        # fallback para stdout
        print(f"[{ts}] {msg}")

DEFAULTS = {
    "SRC": str(BASE_DIR / "entrada"),
    "OUT": str(BASE_DIR / "saida"),
    "PROCESSED": str(BASE_DIR / "processados"),
    "MERGE_ITEMS": False,
    "RECURSIVE": False,
    "PROCESS_EXISTING": True,
    "INDENT": 2,
    "PESO_COL": None,
}
CONFIG_PATH = BASE_DIR / "config.json"

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def load_config() -> Dict[str, Any]:
    cfg = DEFAULTS.copy()
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding='utf-8'))
            for k in DEFAULTS:
                if k in data and data[k] is not None:
                    cfg[k] = data[k]
            log(f"Config carregada de {CONFIG_PATH}")
        except Exception as e:
            log(f"Não foi possível ler config.json ({e}). Usando padrões.")
    else:
        try:
            CONFIG_PATH.write_text(json.dumps(DEFAULTS, ensure_ascii=False, indent=2), encoding='utf-8')
            log(f"Arquivo de configuração criado: {CONFIG_PATH}")
        except Exception as e:
            log(f"Falha ao criar config.json: {e}")
    return cfg

# ==== cria pastas AGORA (antes de watchdog) ====
_cfg = load_config()
for p_key in ("SRC", "OUT", "PROCESSED"):
    try:
        ensure_dir(Path(_cfg[p_key]))
        log(f"Pasta OK: {_cfg[p_key]}")
    except Exception as e:
        log(f"ERRO ao criar pasta {_cfg[p_key]}: {e}")

# tenta importar watchdog
try:
    from watchdog.observers import Observer
    from watchdog.events import PatternMatchingEventHandler
except Exception as e:
    log("ERRO: watchdog não está instalado. Instale com: python -m pip install watchdog")
    raise

# ===== parser/agrupador (igual ao anterior) =====
FIELD_ALIASES = {
    "delivery": ["delivery"],
    "item name": ["item name", "item", "itemname"],
    "ship to customer": ["ship to customer", "shipto customer", "customer"],
    "ship to": ["ship to", "shipto", "ship to address", "deliver to", "deliver-to", "ship_to"],
    "requested": ["requested qty", "requested", "qty requested", "requested quantity"],
    "peso": ["peso", "weight", "peso bruto", "gross weight"],
}

def normalize_header(name: str) -> str:
    return " ".join(name.strip().split()).lower()

def build_header_map(headers: List[str], peso_override: Optional[str]) -> Dict[str, Optional[int]]:
    norm_headers = [normalize_header(h) for h in headers]
    def find_index_for(aliases: List[str]) -> Optional[int]:
        aliases_norm = [normalize_header(a) for a in aliases]
        for i, h in enumerate(norm_headers):
            for a in aliases_norm:
                if h == a:
                    return i
        return None
    mapping = {}
    mapping["delivery"] = find_index_for(FIELD_ALIASES["delivery"])
    mapping["item Name"] = find_index_for(FIELD_ALIASES["item name"])
    mapping["ship to customer"] = find_index_for(FIELD_ALIASES["ship to customer"])
    mapping["ship to"] = find_index_for(FIELD_ALIASES["ship to"])
    mapping["Requested"] = find_index_for(FIELD_ALIASES["requested"])
    peso_idx = None
    if peso_override:
        try:
            peso_idx = norm_headers.index(normalize_header(peso_override))
        except ValueError:
            peso_idx = None
    if peso_idx is None:
        peso_idx = find_index_for(FIELD_ALIASES["peso"])
    mapping["peso"] = peso_idx
    return mapping

def to_number_if_possible(val: Optional[str]):
    if val is None:
        return None
    s = val.strip()
    if s == "":
        return None
    try:
        return int(s)
    except ValueError:
        pass
    try:
        return float(s.replace(',', '.'))
    except ValueError:
        return s

def normalize_cell(s: str) -> Optional[str]:
    v = " ".join(s.strip().split())
    return v if v else None

def read_tsv_grouped(path: Path, *, merge_items: bool, peso_col: Optional[str]) -> List[Dict[str, Any]]:
    rows = list(csv.reader(path.open("r", encoding="utf-8-sig", newline=""), delimiter="\t"))
    if not rows:
        return []
    headers = rows[0]
    data_rows = rows[1:]
    hmap = build_header_map(headers, peso_col)
    groups: Dict[str, Dict[str, Any]] = {}
    for r in data_rows:
        if not any((c or '').strip() for c in r):
            continue
        def get(idx_key: str) -> Optional[str]:
            idx = hmap.get(idx_key)
            if idx is None or idx >= len(r):
                return None
            return normalize_cell(r[idx])
        delivery = get("delivery")
        if not delivery:
            continue
        item_name = get("item Name")
        requested = get("Requested")
        ship_to_customer = get("ship to customer")
        ship_to = get("ship to")
        peso = get("peso")
        g = groups.get(delivery)
        if g is None:
            g = {
                "delivery": delivery,
                "ship to customer": None,
                "ship to": None,
                "items": [],
                "peso": None,
            }
            groups[delivery] = g
        if ship_to_customer and not g["ship to customer"]:
            g["ship to customer"] = ship_to_customer
        if ship_to and not g["ship to"]:
            g["ship to"] = ship_to
        if peso and g["peso"] is None:
            g["peso"] = to_number_if_possible(peso)
        if item_name:
            g["items"].append({
                "item Name": item_name,
                "Requested": to_number_if_possible(requested),
            })
    # MERGE opz
    if _cfg["MERGE_ITEMS"]:
        for g in groups.values():
            acc: Dict[str, float] = {}
            non_numeric: Dict[str, bool] = {}
            for it in g["items"]:
                name = it.get("item Name") or ""
                qty = it.get("Requested")
                if isinstance(qty, (int, float)):
                    acc[name] = acc.get(name, 0.0) + float(qty)
                else:
                    non_numeric[name] = True
            merged = []
            for name, total in acc.items():
                if non_numeric.get(name):
                    merged.append({"item Name": name, "Requested": total})
                else:
                    merged.append({"item Name": name, "Requested": int(total) if float(total).is_integer() else total})
            g["items"] = merged
    result = [groups[k] for k in sorted(groups.keys(), key=lambda x: (x is None, str(x)))]
    return result

def wait_for_file_ready(path: Path, *, checks: int = 10, delay: float = 0.3) -> bool:
    if not path.exists():
        return False
    try:
        last_size = -1
        stable_count = 0
        for _ in range(checks):
            size = path.stat().st_size
            if size == last_size and size > 0:
                stable_count += 1
                if stable_count >= 2:
                    return True
            else:
                stable_count = 0
            last_size = size
            time.sleep(delay)
    except FileNotFoundError:
        return False
    return path.exists() and path.stat().st_size > 0

def process_tsv(tsv_path: Path, out_dir: Path, processed_dir: Path, *, indent: int, peso_col: Optional[str]) -> Optional[Path]:
    if not wait_for_file_ready(tsv_path):
        log(f"[WARN] Arquivo não estabilizou: {tsv_path}")
        return None
    try:
        data = read_tsv_grouped(tsv_path, merge_items=_cfg["MERGE_ITEMS"], peso_col=peso_col)
    except Exception as e:
        log(f"[ERRO] Falha ao ler TSV {tsv_path}: {e}")
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / (tsv_path.stem + ".json")
    try:
        json_str = json.dumps(data, ensure_ascii=False, indent=int(_cfg["INDENT"]))
        json_path.write_text(json_str, encoding="utf-8")
        log(f"[OK] JSON gerado: {json_path} (deliveries: {len(data)})")
    except Exception as e:
        log(f"[ERRO] Falha ao escrever JSON {json_path}: {e}")
        return None
    try:
        processed_dir.mkdir(parents=True, exist_ok=True)
        dest = processed_dir / tsv_path.name
        shutil.move(str(tsv_path), str(dest))
        log(f"[OK] TSV movido para: {dest}")
    except Exception as e:
        log(f"[ERRO] Falha ao mover TSV para 'processados': {e}")
    return json_path

# ---- Watchdog ----
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

class TSVHandler(PatternMatchingEventHandler):
    def __init__(self, *, out: Path, processed: Path, peso_col: Optional[str]):
        super().__init__(patterns=["*.tsv", "*.TSV"], ignore_directories=True, case_sensitive=False)
        self.out = out
        self.processed = processed
        self.peso_col = peso_col
    def on_created(self, event):
        path = Path(event.src_path)
        log(f"[EVENT] created: {path}")
        process_tsv(path, self.out, self.processed, indent=int(_cfg["INDENT"]), peso_col=self.peso_col)
    def on_moved(self, event):
        dest = Path(event.dest_path)
        log(f"[EVENT] moved: {dest}")
        if dest.suffix.lower() == ".tsv":
            process_tsv(dest, self.out, self.processed, indent=int(_cfg["INDENT"]), peso_col=self.peso_col)
    def on_modified(self, event):
        path = Path(event.src_path)
        if path.suffix.lower() == ".tsv":
            log(f"[EVENT] modified: {path}")
            process_tsv(path, self.out, self.processed, indent=int(_cfg["INDENT"]), peso_col=self.peso_col)

def bootstrap() -> None:
    src = Path(_cfg["SRC"]).resolve()
    out = Path(_cfg["OUT"]).resolve()
    processed = Path(_cfg["PROCESSED"]).resolve()
    peso_col = _cfg["PESO_COL"] if _cfg["PESO_COL"] else None

    # processa existentes (se habilitado)
    if bool(_cfg["PROCESS_EXISTING"]):
        files = sorted(src.glob("*.tsv")) + sorted(src.glob("*.TSV"))
        if files:
            log(f"[INFO] Processando {len(files)} TSV(s) existentes...")
        for f in files:
            process_tsv(f, out, processed, indent=int(_cfg["INDENT"]), peso_col=peso_col)

    observer = Observer()
    handler = TSVHandler(out=out, processed=processed, peso_col=peso_col)
    observer.schedule(handler, str(src), recursive=bool(_cfg["RECURSIVE"]))
    observer.start()
    log(f"[START] Monitorando: {src}  (recursive={_cfg['RECURSIVE']}) | Saída: {out} | Processados: {processed}")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log("[STOP] Encerrando monitor...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    bootstrap()