#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask + SQLAlchemy app to browse, reprint, and auto-print delivery reports
- Reads from SQLite DB created by the watcher (tables: deliveries, items)
- Generates a PDF report per delivery (ReportLab)
- Keeps a print history (table: print_log)
- Auto-prints every time a **new delivery** appears in DB (first insert)
- UPDATED: direct Adobe printing (minimized) without needing PRINT_COMMAND

Run:
    python app.py

Install deps:
    python -m pip install flask sqlalchemy reportlab

Env vars (opcionais):
    DB_URL         → default: sqlite:///data/shipments.db (relativo à pasta do app)
    AUTO_PRINT     → "1" para habilitar auto-print (default: 1)
    POLL_SECONDS   → intervalo de polling (segundos) para auto-print (default: 10)
    PRINTER_NAME   → nome da impressora específica (se vazio, usa a padrão)

Notas:
- Em Windows, o app usa diretamente o Adobe Reader (AcroRd32.exe) com 
  switches "/h /t" para imprimir minimizado. Se não encontrado, faz fallback
  para os.startfile(pdf, 'print').
- O histórico de impressões está em 'print_log'.
"""

import os
import time
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from flask import Flask, render_template, send_file, redirect, url_for, flash
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Float, Integer, DateTime,
    ForeignKey, select, func, text
)
from sqlalchemy.orm import registry, Session, Mapped

# ----------- Caminhos base -----------
APP_DIR = Path(__file__).parent.resolve()
DATA_DIR = (APP_DIR / 'data').resolve()
REPORTS_DIR = (APP_DIR / 'reports').resolve()
TEMPLATES_DIR = (APP_DIR / 'templates').resolve()
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ----------- Config -----------
DB_URL = os.getenv('DB_URL') or f"sqlite:///{(DATA_DIR / 'shipments.db').as_posix()}"
AUTO_PRINT = os.getenv('AUTO_PRINT', '1') == '1'
POLL_SECONDS = int(os.getenv('POLL_SECONDS', '10'))
PRINTER_NAME = os.getenv('PRINTER_NAME', '').strip()

# ----------- SQLAlchemy (tabelas já existentes + print_log) -----------
mapper_registry = registry()
engine = create_engine(DB_URL, future=True)
metadata = MetaData()

# Tabelas criadas pelo watcher

deliveries_tbl = Table(
    'deliveries', metadata,
    Column('delivery', String, primary_key=True),
    Column('ship_to_customer', String),
    Column('ship_to', String),
    Column('peso', Float),
    Column('created_by', String),
    Column('created_at', DateTime),
)

items_tbl = Table(
    'items', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('delivery', String, ForeignKey('deliveries.delivery')),
    Column('item_name', String, nullable=False),
    Column('requested', Float),
)

# Tabela auxiliar de histórico de impressões
print_log_tbl = Table(
    'print_log', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('delivery', String, ForeignKey('deliveries.delivery')),
    Column('pdf_path', String),
    Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
)

# Garante existência da tabela print_log (e das outras, se ainda não houve watcher)
metadata.create_all(engine)

# ----------- ORM simples -----------
@mapper_registry.mapped
class Delivery:
    __table__ = deliveries_tbl
    delivery: Mapped[str]
    ship_to_customer: Mapped[Optional[str]]
    ship_to: Mapped[Optional[str]]
    peso: Mapped[Optional[float]]
    created_by: Mapped[Optional[str]]
    created_at: Mapped[Optional[datetime]]

@mapper_registry.mapped
class Item:
    __table__ = items_tbl
    id: Mapped[int]
    delivery: Mapped[str]
    item_name: Mapped[str]
    requested: Mapped[Optional[float]]

@mapper_registry.mapped
class PrintLog:
    __table__ = print_log_tbl
    id: Mapped[int]
    delivery: Mapped[str]
    pdf_path: Mapped[Optional[str]]
    created_at: Mapped[Optional[datetime]]

# ----------- Flask -----------
app = Flask(__name__, template_folder=str(TEMPLATES_DIR))
app.secret_key = os.getenv('FLASK_SECRET_KEY', 'dev-secret')

# ----------- Helpers -----------

def format_weight(peso: Optional[float]) -> str:
    if isinstance(peso, (int, float)):
        # formatação PT-BR simples 1.234,56
        s = f"{peso:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return s
    return ''


def query_deliveries():
    with Session(engine) as s:
        q = (
            s.query(Delivery.delivery, Delivery.ship_to_customer, Delivery.ship_to, Delivery.peso, Delivery.created_by, Delivery.created_at)
            .order_by(Delivery.created_at.desc().nullslast(), Delivery.delivery.desc())
        )
        rows = q.all()
    return rows


def get_delivery(delivery_id: str):
    with Session(engine) as s:
        d = s.get(Delivery, delivery_id)
        if not d:
            return None, []
        items = (
            s.query(Item.item_name, Item.requested)
            .filter(Item.delivery == delivery_id)
            .order_by(Item.item_name)
            .all()
        )
    return d, items

# ----------- PDF (ReportLab) -----------
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import Table as PdfTable, TableStyle, Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.styles import getSampleStyleSheet


def make_delivery_pdf(delivery_id: str) -> Path:
    d, items = get_delivery(delivery_id)
    if not d:
        raise RuntimeError(f"Delivery não encontrado: {delivery_id}")

    pdf_path = REPORTS_DIR / f"delivery_{delivery_id}.pdf"

    styles = getSampleStyleSheet()
    story = []

    title = Paragraph("<b>Relatório de Delivery</b>", styles['Title'])
    story.append(title)
    story.append(Spacer(1, 6*mm))

    info_data = [
        ["Delivery:", d.delivery],
        ["Ship to customer:", d.ship_to_customer or ""],
        ["Ship to:", d.ship_to or ""],
        ["Peso:", format_weight(d.peso)],
        ["Gerado por:", (d.created_by or "")],
        ["Gerado em:", datetime.now().strftime('%d/%m/%Y %H:%M:%S')],
    ]
    info_tbl = PdfTable(info_data, colWidths=[40*mm, 130*mm])
    info_tbl.setStyle(TableStyle([
        ('FONT', (0,0), (-1,-1), 'Helvetica', 10),
        ('INNERGRID', (0,0), (-1,-1), 0.25, colors.grey),
        ('BOX', (0,0), (-1,-1), 0.25, colors.grey),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(info_tbl)
    story.append(Spacer(1, 6*mm))

    # Items
    data = [["Item Name", "Requested"]]
    for name, req in items:
        if isinstance(req, (int, float)):
            req_out = int(req) if (isinstance(req, float) and req.is_integer()) else req
        else:
            req_out = req or ''
        data.append([name or '', str(req_out)])

    tbl = PdfTable(data, colWidths=[120*mm, 50*mm])
    tbl.setStyle(TableStyle([
        ('FONT', (0,0), (-1,-1), 'Helvetica', 10),
        ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
        ('INNERGRID', (0,0), (-1,-1), 0.25, colors.grey),
        ('BOX', (0,0), (-1,-1), 0.25, colors.black),
        ('ALIGN', (1,1), (1,-1), 'RIGHT'),
    ]))
    story.append(tbl)

    doc = SimpleDocTemplate(str(pdf_path), pagesize=A4,
                            leftMargin=15*mm, rightMargin=15*mm,
                            topMargin=15*mm, bottomMargin=15*mm)
    doc.build(story)
    return pdf_path

# ----------- Printing (Adobe direto, minimizado) -----------
import subprocess

def _find_adobe_exe() -> Optional[str]:
    """Tenta localizar o AcroRd32.exe em caminhos comuns."""
    candidates = [
        r"C:\\Program Files\\Adobe\\Acrobat Reader\\Reader\\AcroRd32.exe",
        r"C:\\Program Files\\Adobe\\Acrobat Reader DC\\Reader\\AcroRd32.exe",
        r"C:\\Program Files (x86)\\Adobe\\Acrobat Reader\\Reader\\AcroRd32.exe",
        r"C:\\Program Files (x86)\\Adobe\\Acrobat Reader DC\\Reader\\AcroRd32.exe",
    ]
    for exe in candidates:
        if os.path.exists(exe):
            return exe
    return None


def print_pdf(pdf_path: Path) -> bool:
    """Imprime usando Adobe (minimizado) ou fallback os.startfile('print')."""
    try:
        adobe = _find_adobe_exe()
        if adobe:
            # /h -> minimizado | /t -> imprime | impressora específica opcional
            if PRINTER_NAME:
                cmd = f'"{adobe}" /h /t "{pdf_path}" "{PRINTER_NAME}"'
            else:
                cmd = f'"{adobe}" /h /t "{pdf_path}"'
            subprocess.Popen(cmd, shell=True)
            return True

        # Fallback Windows
        if os.name == 'nt':
            try:
                os.startfile(str(pdf_path), 'print')  # type: ignore[attr-defined]
                return True
            except Exception:
                return False
        return False
    except Exception:
        return False

# ----------- Rotas -----------
@app.route('/')
def index():
    rows = query_deliveries()
    # contagem de itens por delivery
    with Session(engine) as s:
        counts = dict(s.execute(select(Item.delivery, func.count()).group_by(Item.delivery)).all())
    return render_template('index.html', deliveries=rows, counts=counts)

@app.route('/delivery/<delivery_id>')
def delivery_view(delivery_id: str):
    d, items = get_delivery(delivery_id)
    if not d:
        flash('Delivery não encontrado.', 'warning')
        return redirect(url_for('index'))
    return render_template('delivery.html', d=d, items=items)

@app.route('/delivery/<delivery_id>/pdf')
def delivery_pdf(delivery_id: str):
    pdf_path = make_delivery_pdf(delivery_id)
    return send_file(pdf_path, as_attachment=True)

@app.route('/delivery/<delivery_id>/print')
def delivery_print(delivery_id: str):
    pdf_path = make_delivery_pdf(delivery_id)
    printed = print_pdf(pdf_path)
    # log print
    with Session(engine) as s:
        s.execute(print_log_tbl.insert().values(delivery=delivery_id, pdf_path=str(pdf_path)))
        s.commit()
    if printed:
        flash('Impressão enviada para a impressora.', 'success')
    else:
        flash('PDF gerado. Não foi possível enviar para a impressora automaticamente.', 'warning')
    return redirect(url_for('delivery_view', delivery_id=delivery_id))

# ----------- Auto-print daemon -----------

def auto_print_daemon():
    """Verifica deliveries sem registro no print_log e imprime automaticamente."""
    while True:
        try:
            with Session(engine) as s:
                subq = select(print_log_tbl.c.delivery)
                # últimos 50, ordem por created_at desc
                new_deliveries = s.execute(
                    select(Delivery.delivery)
                    .where(~Delivery.delivery.in_(subq))
                    .order_by(Delivery.created_at.desc().nullslast())
                    .limit(50)
                ).scalars().all()
                for delivery_id in new_deliveries:
                    try:
                        pdf_path = make_delivery_pdf(delivery_id)
                        _ = print_pdf(pdf_path)
                        s.execute(print_log_tbl.insert().values(delivery=delivery_id, pdf_path=str(pdf_path)))
                        s.commit()
                    except Exception as e:
                        s.rollback()
                        # log simplificado no console
                        print(f"[auto-print] erro em {delivery_id}: {e}")
        except Exception as e:
            print(f"[auto-print] erro de polling: {e}")
        time.sleep(POLL_SECONDS)

if AUTO_PRINT:
    t = threading.Thread(target=auto_print_daemon, daemon=True)
    t.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
