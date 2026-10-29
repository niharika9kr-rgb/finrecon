import csv
import sqlite3
import pytest
from src.finrecon import connect,generate,ingest,reconcile,run


def test_generator_deterministic(tmp_path):
    a=tmp_path/'a';b=tmp_path/'b';generate(a,80);generate(b,80)
    assert (a/'payments.csv').read_bytes()==(b/'payments.csv').read_bytes()


def test_missing_columns_rejected(tmp_path):
    p=tmp_path/'bad.csv';p.write_text('transaction_id,amount_cents\nX,100\n')
    c=connect(tmp_path/'db.sqlite')
    with pytest.raises(ValueError,match='missing required columns'): ingest(c,p,'payments')
    c.close()


def test_end_to_end_recovery(tmp_path):
    s=run(tmp_path/'job',120)
    assert s['unique_payments']==120
    assert s['duplicate_rejections']==1
    assert s['day1']['PENDING_SETTLEMENT']>s['day2']['PENDING_SETTLEMENT']
    assert s['day2']['MATCHED']>s['day1']['MATCHED']
    assert s['day1']['AMOUNT_MISMATCH']>0
    assert s['day2']['UNEXPECTED_SETTLEMENT']==1


def test_rerun_idempotent(tmp_path):
    source=tmp_path/'source';generate(source,90)
    c=connect(tmp_path/'db.sqlite')
    assert ingest(c,source/'payments.csv','payments')['accepted']==90
    assert ingest(c,source/'payments.csv','payments')['accepted']==0
    assert c.execute('SELECT COUNT(*) FROM payments').fetchone()[0]==90
    c.close()


def test_amount_difference_in_cents(tmp_path):
    source=tmp_path/'source';generate(source,120)
    c=connect(tmp_path/'db.sqlite')
    ingest(c,source/'payments.csv','payments');ingest(c,source/'settlements_day1.csv','settlements')
    reconcile(c,'r1','day1')
    differences=c.execute("SELECT discrepancy_cents FROM reconciliation WHERE status='AMOUNT_MISMATCH'").fetchall()
    assert differences and all(x[0]==500 for x in differences)
    c.close()


def test_audit_history_two_runs(tmp_path):
    run(tmp_path/'job',120)
    c=sqlite3.connect(tmp_path/'job'/'finrecon.sqlite')
    assert c.execute('SELECT COUNT(*) FROM run_history').fetchone()[0]==2
    c.close()


def test_negative_amount_quarantined(tmp_path):
    p=tmp_path/'payments.csv'
    p.write_text('transaction_id,merchant_id,amount_cents,currency,event_date\nX,M1,-1,EUR,2026-10-20\n')
    c=connect(tmp_path/'db.sqlite')
    assert ingest(c,p,'payments')=={'accepted':0,'rejected':1}
    c.close()
