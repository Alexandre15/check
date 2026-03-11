#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Watcher de pasta → Lê TSV e normaliza em duas tabelas:

- deliveries (1 linha por delivery)
- delivery_items (itens agregados por delivery + item_name, somando requested_qty)

Extras:
- (Opcional) shipment_lines (raw) para auditoria (KEEP_RAW=True)
- Debounce de eventos do watchdog
- --oneshot para processar um arquivo e sair
- JSON (auditoria) com default=str para serializar datetime

Dependências:
    python -m pip install watchdog sqlalchemy
"""

import csv
import json
import time
import shutil
import os
import getpass
import traceback
import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

# -------------------- LOG / paths / config --------------------
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

def log_exc(prefix: str, e: Exception) -> None:
    tb = traceback.format_exc()
    log(f"{prefix}: {e}\n{tb}")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def get_current_username() -> str:
    try:
        return os.getlogin()
    except Exception:
        return getpass.getuser() or os.environ.get("USERNAME") or os.environ.get("USER") or "unknown"

DEFAULTS = {
    "SRC": str(BASE_DIR / "entrada"),
    "OUT": str(BASE_DIR / "saida"),
    "PROCESSED": str(BASE_DIR / "processados"),
    "RECURSIVE": False,
    "PROCESS_EXISTING": True,
    "INDENT": 2,
    "PESO_COL": None,      # nome exato da coluna de peso, se existir no TSV
    # DB
    "DB_ENABLED": True,
    "DB_URL": None,        # se None → sqlite:///data/shipments.db
    "SQL_ECHO": False,
    # Anti-duplicação
    "DEBOUNCE_SECONDS": 1.5,
    # Raw opcional
    "KEEP_RAW": False
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
            log_exc("Não foi possível ler config.json. Usando padrões", e)
    else:
        try:
            CONFIG_PATH.write_text(json.dumps(DEFAULTS, ensure_ascii=False, indent=2), encoding='utf-8')
            log(f"Arquivo de configuração criado: {CONFIG_PATH}")
        except Exception as e:
            log_exc("Falha ao criar config.json", e)
    return cfg

_cfg = load_config()
SRC_DIR = Path(_cfg["SRC"]).resolve()
OUT_DIR = Path(_cfg["OUT"]).resolve()
PROCESSED_DIR = Path(_cfg["PROCESSED"]).resolve()
ERRORS_DIR = (BASE_DIR / "falhos").resolve()
for d in (SRC_DIR, OUT_DIR, PROCESSED_DIR, ERRORS_DIR):
    ensure_dir(d)

# -------------------- watchdog --------------------
try:
    from watchdog.observers import Observer
    from watchdog.events import PatternMatchingEventHandler
except Exception:
    log("ERRO: watchdog não está instalado. Instale com: python -m pip install watchdog")
    raise

_last_proc_ts: Dict[str, float] = {}
def _should_process(path: Path) -> bool:
    now = time.time()
    key = str(path.resolve())
    last = _last_proc_ts.get(key, 0.0)
    if (now - last) < float(_cfg.get("DEBOUNCE_SECONDS", 1.5)):
        return False
    _last_proc_ts[key] = now
    return True

# -------------------- SQLAlchemy / DB --------------------
_sa_ok = False
_engine = None
_tables: Dict[str, Any] = {}

try:
    from sqlalchemy import (
        create_engine, MetaData, Table, Column, String, Float, Integer, DateTime,
        ForeignKey, UniqueConstraint, text, func, select
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
    global _engine, _tables, _sa_ok
    if not _sa_ok or not cfg.get("DB_ENABLED", True):
        log("DB: desabilitado ou SQLAlchemy ausente; pulando persistência em banco.")
        return
    try:
        url = get_db_url(cfg)
        _engine = create_engine(url, future=True, echo=bool(cfg.get("SQL_ECHO", False)))
        meta = MetaData()

        # Tabela normalizada - 1 linha por delivery
        deliveries = Table(
            'deliveries', meta,
            Column('delivery', String, primary_key=True),
            Column('ship_to_customer', String),
            Column('ship_to', String),
            Column('peso', Float),
            Column('created_by', String),
            Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
        )

        # Itens agregados por delivery + item_name (soma requested)
        delivery_items = Table(
            'delivery_items', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('delivery', String, ForeignKey('deliveries.delivery', ondelete='CASCADE'), index=True),
            Column('item_name', String, nullable=False),
            Column('requested', Float),
            UniqueConstraint('delivery', 'item_name', name='uq_delivery_item')
        )

        # (Opcional) Raw para auditoria
        shipment_lines = Table(
            'shipment_lines', meta,
            Column('id', Integer, primary_key=True, autoincrement=True),
            Column('source_file', String),
            Column('row_no', Integer),
            Column('delivery', String, index=True),
            Column('item_name', String),
            Column('date_scheduled', DateTime),
            Column('requested_qty', Float),
            Column('shipped_qty', Float),
            Column('backordered_qty', Float),
            Column('peso', Float),
            Column('ship_to_customer', String),
            Column('ship_to', String),
            Column('order_number', String),
            Column('line_status', String),
            Column('next_step', String),
            Column('exceptions', String),
            Column('lpn', String),
            Column('details_required', String),
            Column('serial_number', String),
            Column('inventory_control', String),
            Column('deliver_to', String),
            Column('org_code', String),
            Column('move_order_number', String),
            Column('move_order_line_number', String),
            Column('raw_checkbox', String),
            Column('data_col', String),
            Column('created_by', String),
            Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
        )

        meta.create_all(_engine)

        with _engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deliv_items_delivery ON delivery_items(delivery)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_deliveries_created_at ON deliveries(created_at)"))

            # índices opcionais para raw
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_delivery ON shipment_lines(delivery)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_created_at ON shipment_lines(created_at)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS ix_lines_customer ON shipment_lines(ship_to_customer)"))

        _tables = {
            'deliveries': deliveries,
            'delivery_items': delivery_items,
            'shipment_lines': shipment_lines
        }
        log(f"DB: conectado em {url}; tabelas 'deliveries', 'delivery_items' (e 'shipment_lines' opcional) prontas.")
    except Exception as e:
        log_exc("DB: falha ao inicializar banco", e)
        _engine = None

# -------------------- Parser TSV --------------------
FIELD_ALIASES: Dict[str, List[str]] = {
    "detail": ["detail"],
    "item_name": ["item name", "item", "itemname"],
    "date_scheduled": ["date scheduled", "scheduled date", "data agendada"],
    "requested_qty": ["requested qty", "requested", "qty requested", "requested quantity"],
    "delivery": ["delivery"],
    "ship_to_customer": ["ship to customer", "shipto customer", "customer"],
    "ship_to": ["ship to", "shipto", "ship to address", "deliver to", "deliver-to", "ship_to"],
    "order_number": ["order", "order number", "pedido"],
    "line_status": ["line status"],
    "next_step": ["next step"],
    "exceptions": ["exceptions"],
    "lpn": ["lpn"],
    "shipped_qty": ["shipped qty", "shipped quantity"],
    "backordered_qty": ["backordered qty", "backordered quantity"],
    "details_required": ["details required"],
    "serial_number": ["serial number"],
    "inventory_control": ["inventory control"],
    "data_col": ["data"],
    "deliver_to": ["deliver to"],
    "org_code": ["org code"],
    "move_order_number": ["move order number"],
    "move_order_line_number": ["move order line number"],
    "raw_checkbox": ["[ ]", "checkbox"],
    "peso": ["peso", "weight", "peso bruto", "gross weight"],
}

MONTHS_ENG = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

def normalize_header(name: str) -> str:
    return " ".join((name or "").strip().lower().split())

def build_header_map(headers: List[str], peso_override: Optional[str]) -> Dict[str, Optional[int]]:
    norm = [normalize_header(h) for h in headers]
    def find(aliases: List[str]) -> Optional[int]:
        al = [normalize_header(a) for a in aliases]
        for i, h in enumerate(norm):
            if h in al:
                return i
        return None
    m: Dict[str, Optional[int]] = {}
    for key, aliases in FIELD_ALIASES.items():
        if key == "peso" and peso_override:
            try:
                m["peso"] = norm.index(normalize_header(peso_override))
                continue
            except ValueError:
                pass
        m[key] = find(aliases)
    return m

def normalize_cell(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = " ".join(s.strip().split())
    return v if v else None

def to_number(val: Optional[str]) -> Optional[float]:
    if val is None:
        return None
    s = (val or "").strip()
    if s == "":
        return None
    try:
        # suporta "1.234,56" e "1234.56"
        if s.count(',') == 1 and s.count('.') > 1:
            return float(s.replace('.', '').replace(',', '.'))
        return float(s.replace(',', '.'))
    except Exception:
        try:
            return float(int(s))
        except Exception:
            return None

def parse_oracle_datetime(s: Optional[str]) -> Optional[datetime]:
    """Aceita '13-FEB-2026 00:00:00' (sem depender do locale)."""
    if not s:
        return None
    txt = " ".join(s.strip().split())
    try:
        return datetime.strptime(txt, "%d-%b-%Y %H:%M:%S")
    except Exception:
        pass
    try:
        dpart, tpart = txt.split(" ")
        d, mon, y = dpart.split("-")
        h, m, sec = tpart.split(":")
        mm = MONTHS_ENG.get(mon.upper())
        if not mm:
            return None
        return datetime(int(y), mm, int(d), int(h), int(m), int(sec))
    except Exception:
        return None

def read_tsv_lines(path: Path, peso_col: Optional[str]) -> List[Dict[str, Any]]:
    rows = list(csv.reader(path.open("r", encoding="utf-8-sig", newline=""), delimiter="\t"))
    if not rows:
        return []
    headers = rows[0]
    data = rows[1:]
    hmap = build_header_map(headers, peso_col)
    out: List[Dict[str, Any]] = []

    for idx, r in enumerate(data, start=2):
        if not any((c or '').strip() for c in r):
            continue

        def gv(key: str) -> Optional[str]:
            ix = hmap.get(key)
            if ix is None or ix >= len(r):
                return None
            return normalize_cell(r[ix])

        line = {
            "delivery": gv("delivery"),
            "item_name": gv("item_name"),
            "date_scheduled": parse_oracle_datetime(gv("date_scheduled")),  # datetime or None
            "requested_qty": to_number(gv("requested_qty")),
            "ship_to_customer": gv("ship_to_customer"),
            "ship_to": gv("ship_to"),
            "order_number": gv("order_number"),
            "line_status": gv("line_status"),
            "next_step": gv("next_step"),
            "exceptions": gv("exceptions"),
            "lpn": gv("lpn"),
            "shipped_qty": to_number(gv("shipped_qty")),
            "backordered_qty": to_number(gv("backordered_qty")),
            "details_required": gv("details_required"),
            "serial_number": gv("serial_number"),
            "inventory_control": gv("inventory_control"),
            "deliver_to": gv("deliver_to"),
            "org_code": gv("org_code"),
            "move_order_number": gv("move_order_number"),
            "move_order_line_number": gv("move_order_line_number"),
            "raw_checkbox": gv("raw_checkbox"),
            "data_col": gv("data_col"),
            "peso": to_number(gv("peso")),
            "row_no": idx,
        }
        if not line["delivery"]:
            continue
        out.append(line)
    return out

# -------------------- Normalização / Agregação --------------------
def aggregate_deliveries_and_items(lines: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Retorna:
      - deliveries_data: [{delivery, ship_to_customer, ship_to, peso, created_by, created_at}] (sem created_at, que será preenchido no save)
      - items_data: [{delivery, item_name, requested}] agregando requested_qty
    Regra:
      - ship_to_customer/ship_to/peso: primeiro valor não-nulo encontrado no delivery
      - requested: soma requested_qty por (delivery, item_name)
    """
    deliveries_map: Dict[str, Dict[str, Any]] = {}
    items_acc: Dict[Tuple[str, str], float] = {}

    for ln in lines:
        dv = (ln.get("delivery") or "").strip()
        if not dv:
            continue

        # delivery-level (primeiro não-nulo)
        d = deliveries_map.get(dv)
        if d is None:
            d = {
                "delivery": dv,
                "ship_to_customer": None,
                "ship_to": None,
                "peso": None,
            }
            deliveries_map[dv] = d

        if d["ship_to_customer"] is None and ln.get("ship_to_customer"):
            d["ship_to_customer"] = ln["ship_to_customer"]
        if d["ship_to"] is None and ln.get("ship_to"):
            d["ship_to"] = ln["ship_to"]
        if d["peso"] is None and (ln.get("peso") is not None):
            d["peso"] = ln["peso"]

        # itens (somatório)
        name = ln.get("item_name")
        qty = ln.get("requested_qty")
        if name:
            key = (dv, name)
            if isinstance(qty, (int, float)):
                items_acc[key] = items_acc.get(key, 0.0) + float(qty)

    deliveries_data = []
    for dv, d in deliveries_map.items():
        deliveries_data.append({
            "delivery": dv,
            "ship_to_customer": d["ship_to_customer"],
            "ship_to": d["ship_to"],
            "peso": d["peso"],
        })

    items_data = []
    for (dv, name), total in sorted(items_acc.items(), key=lambda x: (x[0][0], x[0][1])):
        total_out = int(total) if float(total).is_integer() else float(total)
        items_data.append({
            "delivery": dv,
            "item_name": name,
            "requested": total_out
        })

    return deliveries_data, items_data

# -------------------- Persistência --------------------
def wait_for_file_ready(path: Path, *, checks: int = 10, delay: float = 0.3) -> bool:
    if not path.exists():
        return False
    try:
        last = -1
        stable = 0
        for _ in range(checks):
            size = path.stat().st_size
            if size == last and size > 0:
                stable += 1
                if stable >= 2:
                    return True
            else:
                stable = 0
            last = size
            time.sleep(delay)
    except FileNotFoundError:
        return False
    return path.exists() and path.stat().st_size > 0

def _db_count_all_deliveries() -> int:
    try:
        with _engine.begin() as conn:
            res = conn.execute(select(func.count()).select_from(_tables['deliveries'])).scalar()
            return int(res or 0)
    except Exception as e:
        log_exc("Falha ao contar deliveries", e)
        return -1

def save_data_to_db(lines: List[Dict[str, Any]], src_file: Path) -> bool:
    """Insere deliveries + delivery_items (e opcionalmente shipment_lines). Retorna True se OK."""
    if not _engine or not _tables:
        log("[WARN] Engine/tabelas não inicializadas; DB não foi atualizado.")
        return False

    deliveries_tbl = _tables['deliveries']
    items_tbl = _tables['delivery_items']
    lines_tbl = _tables['shipment_lines']
    keep_raw = bool(_cfg.get("KEEP_RAW", False))

    current_user = get_current_username()
    dt_now = datetime.now()

    total_lines = len(lines)
    if total_lines == 0:
        log("[WARN] Nenhuma linha parseada a partir do TSV — nada a gravar no DB.")
        return False

    deliveries_data, items_data = aggregate_deliveries_and_items(lines)
    deliveries = [d["delivery"] for d in deliveries_data]
    log(f"DB: agregação pronta: deliveries={len(deliveries_data)} | items={len(items_data)} | exemplo_delivery={deliveries[0] if deliveries else '—'}")

    try:
        with _engine.begin() as conn:
            # 1) opcional: raw (shipment_lines)
            if keep_raw:
                for dv in sorted(set(deliveries)):
                    conn.execute(lines_tbl.delete().where(lines_tbl.c.delivery == dv))
                raw_rows = []
                for ln in lines:
                    raw_rows.append(dict(
                        source_file=str(src_file.name),
                        row_no=ln.get("row_no"),
                        delivery=ln.get("delivery"),
                        item_name=ln.get("item_name"),
                        date_scheduled=ln.get("date_scheduled"),  # datetime or None
                        requested_qty=ln.get("requested_qty"),
                        shipped_qty=ln.get("shipped_qty"),
                        backordered_qty=ln.get("backordered_qty"),
                        peso=ln.get("peso"),
                        ship_to_customer=ln.get("ship_to_customer"),
                        ship_to=ln.get("ship_to"),
                        order_number=ln.get("order_number"),
                        line_status=ln.get("line_status"),
                        next_step=ln.get("next_step"),
                        exceptions=ln.get("exceptions"),
                        lpn=ln.get("lpn"),
                        details_required=ln.get("details_required"),
                        serial_number=ln.get("serial_number"),
                        inventory_control=ln.get("inventory_control"),
                        deliver_to=ln.get("deliver_to"),
                        org_code=ln.get("org_code"),
                        move_order_number=ln.get("move_order_number"),
                        move_order_line_number=ln.get("move_order_line_number"),
                        raw_checkbox=ln.get("raw_checkbox"),
                        data_col=ln.get("data_col"),
                        created_by=current_user,
                        created_at=dt_now,
                    ))
                if raw_rows:
                    conn.execute(lines_tbl.insert(), raw_rows)

            # 2) deliveries (idempotente: apaga e insere)
            for dv in deliveries:
                conn.execute(items_tbl.delete().where(items_tbl.c.delivery == dv))  # limpar itens antes
                conn.execute(deliveries_tbl.delete().where(deliveries_tbl.c.delivery == dv))

            to_ins_deliv = []
            for d in deliveries_data:
                to_ins_deliv.append(dict(
                    delivery=d["delivery"],
                    ship_to_customer=d.get("ship_to_customer"),
                    ship_to=d.get("ship_to"),
                    peso=(float(d["peso"]) if isinstance(d.get("peso"), (int, float)) else None),
                    created_by=current_user,
                    created_at=dt_now,
                ))
            if to_ins_deliv:
                conn.execute(deliveries_tbl.insert(), to_ins_deliv)

            # 3) items agregados
            to_ins_items = []
            for it in items_data:
                qty = it.get("requested")
                qtyf = float(qty) if isinstance(qty, (int, float)) else None
                to_ins_items.append(dict(
                    delivery=it["delivery"],
                    item_name=it["item_name"],
                    requested=qtyf
                ))
            if to_ins_items:
                conn.execute(items_tbl.insert(), to_ins_items)

        count_after = _db_count_all_deliveries()
        amostra = ", ".join(deliveries[:3]) + ("..." if len(deliveries) > 3 else "")
        log(f"DB: upsert OK: deliveries_ins={len(to_ins_deliv)} | items_ins={len(to_ins_items)} | "
            f"deliveries_total={count_after} | Deliveries(amostra)={amostra}")
        return True

    except Exception as e:
        log_exc("DB: erro ao salvar dados normalizados", e)
        return False

# -------------------- Pipeline --------------------
def process_tsv(tsv_path: Path, out_dir: Path, processed_dir: Path, *, indent: int, peso_col: Optional[str]) -> Optional[Path]:
    # Debounce
    try:
        if not _should_process(tsv_path):
            log(f"[INFO] Debounced (ignorado evento repetido): {tsv_path}")
            return None
    except Exception as e:
        log_exc("Debounce falhou", e)

    if not wait_for_file_ready(tsv_path):
        log(f"[WARN] Arquivo não estabilizou: {tsv_path}")
        return None

    # Parse linhas
    try:
        lines = read_tsv_lines(tsv_path, peso_col=peso_col)
        log(f"[INFO] Parse OK: {tsv_path.name} → {len(lines)} linha(s)")
    except Exception as e:
        log_exc(f"[ERRO] Falha ao ler TSV {tsv_path}", e)
        return None

    # JSON auditoria
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_obj = {"generated_at": now_str, "source_file": tsv_path.name, "lines": lines}
    ensure_dir(out_dir)
    json_path = out_dir / (tsv_path.stem + ".json")
    try:
        json_path.write_text(json.dumps(out_obj, ensure_ascii=False, indent=int(_cfg["INDENT"]), default=str), encoding="utf-8")
        log(f"[OK] JSON gerado: {json_path} (linhas: {len(lines)})")
    except Exception as e:
        log_exc(f"[ERRO] Escrevendo JSON {json_path}", e)
        # não retorna; ainda tentamos gravar no DB

    # DB (deliveries + delivery_items [+ raw opcional])
    saved_ok = False
    try:
        saved_ok = save_data_to_db(lines, tsv_path)
    except Exception as e:
        saved_ok = False
        log_exc("Erro em save_data_to_db", e)

    # mover TSV
    try:
        dest = (processed_dir if saved_ok else (BASE_DIR / "falhos")) / tsv_path.name
        ensure_dir(dest.parent)
        shutil.move(str(tsv_path), str(dest))
        log(f"[OK] TSV movido para: {dest} (saved_ok={saved_ok})")
    except Exception as e:
        log_exc("Falha ao mover TSV pós-processamento", e)

    return json_path

# -------------------- Watchdog handler --------------------
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

# -------------------- Bootstrap / CLI --------------------
def process_existing(src: Path, out: Path, processed: Path, *, indent: int, peso_col: Optional[str]):
    files = sorted(list(src.glob("*.tsv")) + list(src.glob("*.TSV")))
    if files:
        log(f"[INFO] Processando {len(files)} TSV(s) existentes...")
    for f in files:
        process_tsv(f, out, processed, indent=indent, peso_col=peso_col)

def watch_loop():
    observer = Observer()
    handler = TSVHandler(out=OUT_DIR, processed=PROCESSED_DIR, peso_col=_cfg["PESO_COL"])
    observer.schedule(handler, str(SRC_DIR), recursive=bool(_cfg["RECURSIVE"]))
    observer.start()
    log(f"[START] Monitorando: {SRC_DIR} (recursive={_cfg['RECURSIVE']}) | Saída: {OUT_DIR} | Processados: {PROCESSED_DIR}")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        log("[STOP] Encerrando monitor...")
        observer.stop()
    observer.join()

def main():
    parser = argparse.ArgumentParser(description="Watcher TSV → deliveries + delivery_items (+ raw opcional)")
    parser.add_argument("--oneshot", help="Processa um único arquivo .tsv e sai", default=None)
    parser.add_argument("--src", help="Sobrescreve SRC do config", default=None)
    parser.add_argument("--db-url", help="Sobrescreve DB_URL do config", default=None)
    parser.add_argument("--no-existing", help="Não processa arquivos já existentes na pasta SRC", action="store_true")
    parser.add_argument("--no-watch", help="Não inicia o watchdog; apenas processa existentes (se habilitado)", action="store_true")
    args = parser.parse_args()

    # Overrides
    if args.src:
        global SRC_DIR
        SRC_DIR = Path(args.src).resolve()
        ensure_dir(SRC_DIR)
        _cfg["SRC"] = str(SRC_DIR)

    if args.db_url:
        _cfg["DB_URL"] = args.db_url

    init_db(_cfg)

    if args.oneshot:
        tsv = Path(args.oneshot).resolve()
        if not tsv.exists():
            log(f"[ERRO] Arquivo não encontrado: {tsv}")
            return
        process_tsv(tsv, OUT_DIR, PROCESSED_DIR, indent=int(_cfg["INDENT"]), peso_col=_cfg["PESO_COL"])
        return

    if not args.no_existing and bool(_cfg["PROCESS_EXISTING"]):
        process_existing(SRC_DIR, OUT_DIR, PROCESSED_DIR, indent=int(_cfg["INDENT"]), peso_col=_cfg["PESO_COL"])

    if not args.no_watch:
        watch_loop()
    else:
        log("[INFO] Execução concluída sem iniciar watchdog (--no-watch).")

if __name__ == "__main__":
    main()