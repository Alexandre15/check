#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flask + SQLAlchemy baseado em tabelas normalizadas:
- deliveries (1 linha por delivery)
- delivery_items (itens agregados por delivery + item_name)
- print_log (histórico de impressões)

Recursos:
- Lista de deliveries, detalhamento e PDF
- Impressão (Adobe minimizado) e auto-print
- Dashboard com filtros (data/cliente) + timezone America/Sao_Paulo
- KPIs + Entregas por dia + Top clientes (pizza) + Ranking HOJE por usuário

Watcher: deve popular deliveries + delivery_items
"""

import os
import time
import threading
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Optional, Tuple, List, Dict

from flask import Flask, render_template, send_file, redirect, url_for, flash, request
from zoneinfo import ZoneInfo

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Float, Integer, DateTime,
    ForeignKey, UniqueConstraint, select, func, text, distinct
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

# Se o watcher gravar created_at em UTC, ajuste para 1; caso seja horário local, deixe 0
DB_TIME_IS_UTC = os.getenv('DB_TIME_IS_UTC', '0') == '1'

APP_TZ = ZoneInfo("America/Sao_Paulo")
UTC = ZoneInfo("UTC")

# ----------- SQLAlchemy -----------
mapper_registry = registry()
engine = create_engine(DB_URL, future=True)
metadata = MetaData()

# Tabelas normalizadas
deliveries_tbl = Table(
    'deliveries', metadata,
    Column('delivery', String, primary_key=True),
    Column('ship_to_customer', String),
    Column('ship_to', String),
    Column('peso', Float),
    Column('created_by', String),
    Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
)

delivery_items_tbl = Table(
    'delivery_items', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('delivery', String, ForeignKey('deliveries.delivery', ondelete='CASCADE'), index=True),
    Column('item_name', String, nullable=False),
    Column('requested', Float),
    UniqueConstraint('delivery', 'item_name', name='uq_delivery_item')
)

print_log_tbl = Table(
    'print_log', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('delivery', String),
    Column('pdf_path', String),
    Column('created_at', DateTime, server_default=text('CURRENT_TIMESTAMP')),
)

# Cria as tabelas se não existirem (compatível com watcher)
metadata.create_all(engine)

# (Opcional) Garantir foreign_keys ON para SQLite (cascade nativo)
try:
    from sqlalchemy import event
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, conn_record):
        try:
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()
        except Exception:
            pass
except Exception:
    pass

# ----------- ORM -----------
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
class DeliveryItem:
    __table__ = delivery_items_tbl
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

# ----------- Timezone helpers -----------

def _parse_date(s: Optional[str]) -> Optional[date]:
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None

def _local_day_bounds(d: date) -> Tuple[datetime, datetime]:
    start_local = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=APP_TZ)
    next_local = start_local + timedelta(days=1)
    return start_local, next_local

def _to_db_range(start_local: datetime, end_local: datetime) -> Tuple[datetime, datetime]:
    if DB_TIME_IS_UTC:
        s = start_local.astimezone(UTC).replace(tzinfo=None)
        e = end_local.astimezone(UTC).replace(tzinfo=None)
    else:
        s = start_local.replace(tzinfo=None)
        e = end_local.replace(tzinfo=None)
    return s, e

def _sqlite_local_day_expr():
    # offset atual em horas (sem DST no BR)
    off = int((APP_TZ.utcoffset(datetime.now(APP_TZ)) or timedelta(0)).total_seconds() // 3600)
    if DB_TIME_IS_UTC:
        return func.strftime('%Y-%m-%d', func.datetime(Delivery.created_at, f'{off:+d} hours'))
    else:
        return func.strftime('%Y-%m-%d', Delivery.created_at)

# ----------- Helpers / Queries -----------

def query_deliveries(filters: Optional[List] = None):
    with Session(engine) as s:
        q = select(
            Delivery.delivery,
            Delivery.ship_to_customer,
            Delivery.ship_to,
            Delivery.peso,
            Delivery.created_by,
            Delivery.created_at
        )
        if filters:
            q = q.where(*filters)
        q = q.order_by(Delivery.created_at.desc().nullslast(), Delivery.delivery.desc())
        rows = s.execute(q).all()
    return rows

def get_delivery(delivery_id: str):
    with Session(engine) as s:
        d = s.get(Delivery, delivery_id)
        if not d:
            return None, []
        items = s.execute(
            select(DeliveryItem.item_name, DeliveryItem.requested)
            .where(DeliveryItem.delivery == delivery_id)
            .order_by(DeliveryItem.item_name)
        ).all()
    return d, items

# ----------- PDF (ReportLab) -----------
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.platypus import Table as PdfTable, TableStyle, Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.styles import getSampleStyleSheet

def _fmt_weight(peso: Optional[float]) -> str:
    if isinstance(peso, (int, float)):
        s = f"{peso:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return s
    return ''

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
        ["Peso:", _fmt_weight(d.peso)],
        ["Gerado por:", (d.created_by or "")],
        ["Gerado em:", datetime.now(APP_TZ).strftime('%d/%m/%Y %H:%M:%S %Z')],
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

    # Itens
    data = [["Item Name", "Requested"]]
    for name, req in items:
        if isinstance(req, (int, float)):
            req_out = int(req) if (isinstance(req, float) and float(req).is_integer()) else req
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
    candidates = [
        r"C:\Program Files\Adobe\Acrobat Reader\Reader\AcroRd32.exe",
        r"C:\Program Files\Adobe\Acrobat Reader DC\Reader\AcroRd32.exe",
        r"C:\Program Files (x86)\Adobe\Acrobat Reader\Reader\AcroRd32.exe",
        r"C:\Program Files (x86)\Adobe\Acrobat Reader DC\Reader\AcroRd32.exe",
    ]
    for exe in candidates:
        if os.path.exists(exe):
            return exe
    return None

def print_pdf(pdf_path: Path) -> bool:
    try:
        adobe = _find_adobe_exe()
        if adobe:
            if PRINTER_NAME:
                cmd = f'"{adobe}" /h /t "{pdf_path}" "{PRINTER_NAME}"'
            else:
                cmd = f'"{adobe}" /h /t "{pdf_path}"'
            subprocess.Popen(cmd, shell=True)
            return True

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
        counts = dict(
            s.execute(
                select(DeliveryItem.delivery, func.count(DeliveryItem.id))
                .group_by(DeliveryItem.delivery)
            ).all()
        )
    return render_template('index.html', deliveries=rows, counts=counts, now=datetime.now(APP_TZ), DB_URL=DB_URL)

@app.route('/delivery/<delivery_id>')
def delivery_view(delivery_id: str):
    d, items = get_delivery(delivery_id)
    if not d:
        flash('Delivery não encontrado.', 'warning')
        return redirect(url_for('index'))
    return render_template('delivery.html', d=d, items=items, now=datetime.now(APP_TZ), DB_URL=DB_URL)

@app.route('/delivery/<delivery_id>/pdf')
def delivery_pdf(delivery_id: str):
    pdf_path = make_delivery_pdf(delivery_id)
    return send_file(pdf_path, as_attachment=True)

@app.route('/delivery/<delivery_id>/print')
def delivery_print(delivery_id: str):
    pdf_path = make_delivery_pdf(delivery_id)
    printed = print_pdf(pdf_path)
    with Session(engine) as s:
        s.execute(print_log_tbl.insert().values(delivery=delivery_id, pdf_path=str(pdf_path)))
        s.commit()
    if printed:
        flash('Impressão enviada para a impressora.', 'success')
    else:
        flash('PDF gerado. Não foi possível enviar para a impressora automaticamente.', 'warning')
    return redirect(url_for('delivery_view', delivery_id=delivery_id))

# ----------- Dashboard -----------

def build_dashboard_context(date_from: Optional[date],
                            date_to: Optional[date],
                            customer: Optional[str]):
    where = []
    if date_from and date_to and date_from > date_to:
        date_from, date_to = date_to, date_from

    if date_from:
        f_start_local, _ = _local_day_bounds(date_from)
    if date_to:
        _, t_next_local = _local_day_bounds(date_to)

    if date_from and date_to:
        s_db, e_db = _to_db_range(f_start_local, t_next_local)
        where.extend([Delivery.created_at >= s_db, Delivery.created_at < e_db])
    elif date_from:
        s_db, _ = _to_db_range(f_start_local, f_start_local)
        where.append(Delivery.created_at >= s_db)
    elif date_to:
        _, e_db = _to_db_range(*_local_day_bounds(date_to))
        where.append(Delivery.created_at < e_db)

    if customer:
        where.append(Delivery.ship_to_customer == customer)

    with Session(engine) as s:
        # Conjunto filtrado base de deliveries
        base = select(Delivery.delivery).where(*where) if where else select(Delivery.delivery)
        base = base.subquery()

        # KPIs
        total_deliveries = s.execute(select(func.count()).select_from(base)).scalar() or 0

        # total_items: número de linhas em delivery_items associados às deliveries filtradas
        items_q = select(func.count(DeliveryItem.id)).where(DeliveryItem.delivery.in_(select(base.c.delivery)))
        total_items = s.execute(items_q).scalar() or 0

        # total_weight: soma de peso das deliveries filtradas
        wq = select(func.coalesce(func.sum(Delivery.peso), 0.0))
        if where:
            wq = wq.where(*where)
        total_weight = float(s.execute(wq).scalar() or 0.0)

        # printed distintos
        printed = s.execute(
            select(func.count(distinct(print_log_tbl.c.delivery)))
            .where(print_log_tbl.c.delivery.in_(select(base.c.delivery)))
        ).scalar() or 0
        backlog = max(int(total_deliveries) - int(printed), 0)

        # Entregas por dia (local)
        day_expr = _sqlite_local_day_expr()
        deliveries_by_day_q = select(day_expr.label('day'), func.count(Delivery.delivery))
        if where:
            deliveries_by_day_q = deliveries_by_day_q.where(*where)
        deliveries_by_day_q = deliveries_by_day_q.group_by('day').order_by('day')
        deliveries_by_day = s.execute(deliveries_by_day_q).all()

        # Top clientes (pie) → count deliveries
        top_customers_q = select(Delivery.ship_to_customer, func.count(Delivery.delivery))
        if where:
            top_customers_q = top_customers_q.where(*where)
        top_customers_q = top_customers_q.group_by(Delivery.ship_to_customer) \
                                         .order_by(func.count(Delivery.delivery).desc()) \
                                         .limit(10)
        top_customers = s.execute(top_customers_q).all()

        # Ranking HOJE por usuário (independente dos filtros)
        today_local = datetime.now(APP_TZ).date()
        r_start_local, r_next_local = _local_day_bounds(today_local)
        r_s_db, r_e_db = _to_db_range(r_start_local, r_next_local)
        ranking_today = s.execute(
            select(Delivery.created_by, func.count(Delivery.delivery))
            .where(Delivery.created_at >= r_s_db, Delivery.created_at < r_e_db)
            .group_by(Delivery.created_by)
            .order_by(func.count(Delivery.delivery).desc(), Delivery.created_by.asc())
        ).all()

        # Lista de clientes para filtros
        customers = s.execute(
            select(Delivery.ship_to_customer)
            .group_by(Delivery.ship_to_customer)
            .order_by(Delivery.ship_to_customer.asc())
            .limit(1000)
        ).scalars().all()

    charts = {
        "deliveries_by_day": [["Dia", "Entregas"]] + [[d or "—", int(c)] for d, c in deliveries_by_day],
        "top_customers_pie": [["Cliente", "Entregas"]] + [[c or "—", int(q or 0)] for c, q in top_customers],
    }
    kpis = {
        "total_deliveries": int(total_deliveries),
        "total_items": int(total_items),
        "total_weight": float(total_weight or 0.0),
        "printed": int(printed),
        "backlog": int(backlog),
    }
    ranking_today_out = [{"user": (u or "—"), "qtd": int(q or 0)} for u, q in ranking_today]
    ui_filters = {
        "from": date_from.isoformat() if date_from else "",
        "to": date_to.isoformat() if date_to else "",
        "customer": customer or "",
        "db_time_is_utc": DB_TIME_IS_UTC,
    }
    return charts, kpis, ranking_today_out, customers, ui_filters

@app.route('/dashboard')
def dashboard():
    q_from = request.args.get('from') or request.args.get('start') or ""
    q_to = request.args.get('to') or request.args.get('end') or ""
    q_customer = request.args.get('customer') or ""

    dfrom = _parse_date(q_from)
    dto = _parse_date(q_to)

    charts, kpis, ranking_today, customers, ui = build_dashboard_context(dfrom, dto, q_customer)
    return render_template(
        'dashboard.html',
        charts=charts,
        kpis=kpis,
        ranking_today=ranking_today,
        customers=customers,
        filters=ui,
        now=datetime.now(APP_TZ),
        DB_URL=DB_URL
    )

# ----------- Auto-print daemon -----------

def auto_print_daemon():
    while True:
        try:
            with Session(engine) as s:
                new_deliveries = s.execute(
                    select(Delivery.delivery)
                    .where(~Delivery.delivery.in_(select(print_log_tbl.c.delivery)))
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
                        print(f"[auto-print] erro em {delivery_id}: {e}")
        except Exception as e:
            print(f"[auto-print] erro de polling: {e}")
        time.sleep(POLL_SECONDS)

if AUTO_PRINT:
    t = threading.Thread(target=auto_print_daemon, daemon=True)
    t.start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)