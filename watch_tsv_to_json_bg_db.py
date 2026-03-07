#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitor de pasta (watchdog) → Converte TSV em JSON (agrupado por Delivery) e persiste em SQLite via SQLAlchemy.
Regras:
- Agrupa por Delivery (1x por delivery).
- Em cada delivery: pega 1x "ship to customer", 1x "ship to", 1x "peso" (primeiro valor não vazio).
- Itens: pares { "item Name", "Requested" }. Ignora itens sem "item Name".
- MERGE_ITEMS (config) opcional: soma quantidades de itens com o mesmo nome por delivery.
- JSON e DB recebem "created_by" (username do PC atual).
- DB recebe "created_at":
    - Forçado em INSERT (func.current_timestamp()).
    - Backfill em UPDATE quando nulo.

Uso (modo clique): basta rodar este arquivo (pastas padrão são criadas ao lado).
Config via config.json (gerado automaticamente na 1ª execução).
"""

import csv
import json
import sys
import time
import shutil
import os
import getpass
from pathlib import Path
from typing import Any, Dict, List, Optional
from collections import defaultdict
from datetime import datetime

# --------------- LOG / paths / config ---------------
BASE_DIR = Path(__file__).parent.resolve()
LOG_PATH = BASE_DIR / "monitor.log"
CONFIG_PATH = BASE_DIR / "config.json"

def log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    try:
        with LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        print(f"[{ts}] {msg}")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def get_current_username() -> str:
    """
    Retorna o nome de usuário da sessão atual (Windows/Linux).
    """
    try:
        return os.getlogin()
    except Exception:
        try:
            return getpass.getuser()
        except Exception:
            return os.environ.get("USERNAME") or os.environ.get("USER") or "unknown"

DEFAULTS = {
    "SRC": str(BASE_DIR / "entrada"),
    "OUT": str(BASE_DIR / "saida"),
    "PROCESSED": str(BASE_DIR / "processados"),
    "MERGE_ITEMS": False,
    "RECURSIVE": False,
    "PROCESS_EXISTING": True,
    "INDENT": 2,
    "PESO_COL": None,
    # DB
    "DB_ENABLED": True,
    "DB_URL": None,   # se None → sqlite:///data/shipments.db relativo ao script
}

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

_cfg = load_config()
SRC_DIR = Path(_cfg["SRC"]).resolve()
OUT_DIR = Path(_cfg["OUT"]).resolve()
PROCESSED_DIR = Path(_cfg["PROCESSED"]).resolve()
for d in (SRC_DIR, OUT_DIR, PROCESSED_DIR):
    ensure_dir(d)

# --------------- watchdog ---------------
try:
    from watchdog.observers import Observer
    from watchdog.events import PatternMatchingEventHandler
except Exception:
    log("ERRO: watchdog não está instalado. Instale com: python -m pip install watchdog")
    raise

# --------------- SQLAlchemy / DB ---------------
_sa_ok = False
_engine = None
_tables: Dict[str, Any] = {}

try:
    from sqlalchemy import (
        create_engine, MetaData, Table, Column, String, Float, Integer, DateTime,
        ForeignKey, UniqueConstraint, text, func
    )
    _sa_ok = True
except Exception:
    log("WARN: SQLAlchemy não está instalado. Instale com: python -m pip install sqlalchemy")
    _sa_ok = False

def get_db_url(cfg: Dict[str, Any]) -> str:
    url = cfg.get("DB_URL")
    if url:
        return url
    data_dir = BASE_DIR / "data"
    ensure_dir(data_dir)
    db_path = data_dir / "shipments.db"
    return f"sqlite:///{db_path.as_posix()}"

def init_db(cfg: Dict[str, Any]):
    """
    Cria engine, define metadados e garante colunas (inclui created_by).
    """
    global _engine, _tables, _sa_ok
    if not _sa_ok or not cfg.get("DB_ENABLED", True):
        log("DB: desabilitado ou SQLAlchemy ausente; pulando persistência em banco.")
        return
    try:
        url = get_db_url(cfg)
        _engine = create_engine(url, future=True)
        meta = MetaData()

        deliveries = Table(
            'deliveries', meta,
            Column('delivery', String, primary_key=True),
            Column('ship_to_customer', String),
            Column('ship_to', String),
            Column('peso', Float),
            Column('created_by', String),  # NOVO campo (se não existir, tentamos ADD abaixo)
            Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
        )
        items = Table(
            'items', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('delivery', String, ForeignKey('deliveries.delivery', ondelete='CASCADE')),
            Column('item_name', String, nullable=False),
            Column('requested', Float),
            UniqueConstraint('delivery', 'item_name', name='uq_delivery_item')
        )

        meta.create_all(_engine)

        # Tenta adicionar created_by se a tabela já existia sem a coluna
        with _engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE deliveries ADD COLUMN created_by VARCHAR"))
                log("DB: coluna 'created_by' adicionada em 'deliveries'.")
            except Exception:
                pass  # já existe

        _tables = {'deliveries': deliveries, 'items': items}
        log(f"DB: conectado em {url} e tabelas prontas.")
    except Exception as e:
        log(f"DB: falha ao inicializar banco: {e}")
        _engine = None

# --------------- Parser TSV e agrupamento ---------------
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
        ali_norm = [normalize_header(a) for a in aliases]
        for i, h in enumerate(norm_headers):
            for a in ali_norm:
                if h == a:
                    return i
        return None

    mapping: Dict[str, Optional[int]] = {}
    mapping["delivery"] = find_index_for(FIELD_ALIASES["delivery"])
    mapping["item Name"] = find_index_for(FIELD_ALIASES["item name"])
    mapping["ship to customer"] = find_index_for(FIELD_ALIASES["ship to customer"])
    mapping["ship to"] = find_index_for(FIELD_ALIASES["ship to"])
    mapping["Requested"] = find_index_for(FIELD_ALIASES["requested"])

    # peso: override > alias
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

def to_number_if_possible(val: Optional[str]) -> Optional[Any]:
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

        # Ignora itens sem nome
        if item_name:
            g["items"].append({
                "item Name": item_name,
                "Requested": to_number_if_possible(requested),
            })

    # MERGE opcional
    if merge_items:
        def norm_name(name: Optional[str]) -> str:
            if not name:
                return ""
            return " ".join(name.strip().split()).upper()

        for g in groups.values():
            acc_num: Dict[str, float] = {}
            keep_label: Dict[str, str] = {}
            for it in g["items"]:
                raw = it.get("item Name")
                key = norm_name(raw)
                qty = it.get("Requested")
                if key and key not in keep_label and raw:
                    keep_label[key] = " ".join(raw.strip().split())
                if isinstance(qty, (int, float)):
                    acc_num[key] = acc_num.get(key, 0.0) + float(qty)
                # se qty não numérico, ignoramos para somatório, mas manteríamos se você quisesse listar separado
            merged = []
            for key, total in acc_num.items():
                label = keep_label.get(key, key)
                total_out = int(total) if float(total).is_integer() else total
                merged.append({"item Name": label, "Requested": total_out})
            g["items"] = sorted(merged, key=lambda x: x["item Name"])

    # resultado ordenado por delivery
    result = [groups[k] for k in sorted(groups.keys(), key=lambda x: (x is None, str(x)))]
    return result

# --------------- Persistência em DB ---------------
def save_to_db(deliveries_data: List[Dict[str, Any]]):
    """
    Upsert em deliveries e reconstrução de items por delivery.
    Força created_at no INSERT e backfill no UPDATE quando nulo.
    """
    if not _engine or not _tables:
        return
    deliveries = _tables['deliveries']
    items = _tables['items']
    try:
        with _engine.begin() as conn:
            for d in deliveries_data:
                delivery = (d.get('delivery') or '').strip()
                if not delivery:
                    continue
                ship_to_customer = d.get('ship to customer')
                ship_to = d.get('ship to')
                peso = d.get('peso')
                created_by = d.get('created_by')

                # UPDATE
                upd = conn.execute(
                    deliveries.update()
                    .where(deliveries.c.delivery == delivery)
                    .values(
                        ship_to_customer=ship_to_customer,
                        ship_to=ship_to,
                        peso=float(peso) if isinstance(peso, (int, float)) else None,
                        created_by=created_by,
                    )
                )

                if upd.rowcount == 0:
                    # INSERT (força timestamp)
                    conn.execute(
                        deliveries.insert().values(
                            delivery=delivery,
                            ship_to_customer=ship_to_customer,
                            ship_to=ship_to,
                            peso=float(peso) if isinstance(peso, (int, float)) else None,
                            created_by=created_by,
                            created_at=func.current_timestamp(),  # => garante valor
                        )
                    )
                else:
                    # BACKFILL se created_at é NULL
                    conn.execute(
                        deliveries.update()
                        .where(
                            (deliveries.c.delivery == delivery) &
                            (deliveries.c.created_at.is_(None))
                        )
                        .values(created_at=func.current_timestamp())
                    )

                # Recria itens do delivery
                conn.execute(items.delete().where(items.c.delivery == delivery))
                for it in (d.get('items') or []):
                    name = it.get('item Name')
                    if not name:
                        continue
                    qty = it.get('Requested')
                    qtyf = float(qty) if isinstance(qty, (int, float)) else None
                    conn.execute(items.insert().values(
                        delivery=delivery,
                        item_name=name,
                        requested=qtyf,
                    ))
        log(f"DB: persistidos {len(deliveries_data)} delivery(ies).")
    except Exception as e:
        log(f"DB: erro ao salvar dados: {e}")

# --------------- Pipeline de processamento ---------------
def wait_for_file_ready(path: Path, *, checks: int = 10, delay: float = 0.3) -> bool:
    """
    Espera o arquivo estabilizar (tamanho igual em duas checagens consecutivas).
    """
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
    """
    Processa um arquivo TSV: gera JSON, salva no DB e move o TSV para 'processados'.
    """
    if not wait_for_file_ready(tsv_path):
        log(f"[WARN] Arquivo não estabilizou: {tsv_path}")
        return None

    try:
        data = read_tsv_grouped(tsv_path, merge_items=_cfg["MERGE_ITEMS"], peso_col=peso_col)
    except Exception as e:
        log(f"[ERRO] Falha ao ler TSV {tsv_path}: {e}")
        return None

    # Injeta created_by e created_at no JSON
    current_user = get_current_username()
    current_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for d in data:
        d["created_by"] = current_user
        d["created_at"] = current_ts  # timestamp do processamento no JSON

    # 1) Grava JSON
    ensure_dir(out_dir)
    json_path = out_dir / (tsv_path.stem + '.json')
    try:
        json_str = json.dumps(data, ensure_ascii=False, indent=int(_cfg["INDENT"]))
        json_path.write_text(json_str, encoding='utf-8')
        log(f"[OK] JSON gerado: {json_path} (deliveries: {len(data)})")
    except Exception as e:
        log(f"[ERRO] Falha ao escrever JSON {json_path}: {e}")
        return None

    # 2) Persistência no DB
    save_to_db(data)

    # 3) Move TSV para 'processados'
    try:
        ensure_dir(processed_dir)
        dest = processed_dir / tsv_path.name
        shutil.move(str(tsv_path), str(dest))
        log(f"[OK] TSV movido para: {dest}")
    except Exception as e:
        log(f"[ERRO] Falha ao mover TSV para 'processados': {e}")

    return json_path

# --------------- Watchdog Handler ---------------
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
        if dest.suffix.lower() == '.tsv':
            process_tsv(dest, self.out, self.processed, indent=int(_cfg["INDENT"]), peso_col=self.peso_col)

    def on_modified(self, event):
        path = Path(event.src_path)
        if path.suffix.lower() == '.tsv':
            log(f"[EVENT] modified: {path}")
            process_tsv(path, self.out, self.processed, indent=int(_cfg["INDENT"]), peso_col=self.peso_col)

# --------------- Bootstrap ---------------
def process_existing(src: Path, out: Path, processed: Path, *, indent: int, peso_col: Optional[str]):
    files = sorted(src.glob("*.tsv")) + sorted(src.glob("*.TSV"))
    if files:
        log(f"[INFO] Processando {len(files)} TSV(s) existentes...")
    for f in files:
        process_tsv(f, out, processed, indent=indent, peso_col=peso_col)

def bootstrap() -> None:
    # Inicializa DB
    init_db(_cfg)

    peso_col = _cfg["PESO_COL"] if _cfg["PESO_COL"] else None

    if bool(_cfg["PROCESS_EXISTING"]):
        process_existing(SRC_DIR, OUT_DIR, PROCESSED_DIR, indent=int(_cfg["INDENT"]), peso_col=peso_col)

    observer = Observer()
    handler = TSVHandler(out=OUT_DIR, processed=PROCESSED_DIR, peso_col=peso_col)
    observer.schedule(handler, str(SRC_DIR), recursive=bool(_cfg["RECURSIVE"]))
    observer.start()
    log(f"[START] Monitorando: {SRC_DIR}  (recursive={_cfg['RECURSIVE']}) | Saída: {OUT_DIR} | Processados: {PROCESSED_DIR}")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log("[STOP] Encerrando monitor...")
        observer.stop()
    observer.join()

if __name__ == "__main__":
    bootstrap()