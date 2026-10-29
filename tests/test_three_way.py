import sqlite3
from src.finrecon import connect, generate, ingest, reconcile, run


def _setup(tmp_path, n=120):
    src=tmp_path/'src'; generate(src,n)
    c=connect(tmp_path/'test.db')
    for kind,name in [('payments','payments.csv'),('settlements','settlements_day1.csv'),('payouts','payouts.csv')]:
        ingest(c,src/name,kind)
    return c,src


def test_payouts_are_actually_reconciled(tmp_path):
    c,_=_setup(tmp_path)
    reconcile(c,'one','day1')
    assert c.execute("SELECT COUNT(*) FROM reconciliation WHERE payout_status='PAYOUT_MATCHED'").fetchone()[0] > 0
    assert c.execute("SELECT COUNT(*) FROM reconciliation WHERE payout_status='PENDING_PAYOUT'").fetchone()[0] > 0
    c.close()


def test_payout_amount_mismatch_detected(tmp_path):
    c,_=_setup(tmp_path)
    c.execute("UPDATE payouts SET payout_cents=payout_cents-125 WHERE transaction_id='TXN000001'")
    reconcile(c,'one','day1')
    assert c.execute("SELECT payout_status,payout_discrepancy_cents FROM reconciliation WHERE transaction_id='TXN000001'").fetchone()==('PAYOUT_AMOUNT_MISMATCH',125)
    c.close()


def test_payout_merchant_mismatch_detected(tmp_path):
    c,_=_setup(tmp_path)
    c.execute("UPDATE payouts SET merchant_id='WRONG' WHERE transaction_id='TXN000001'")
    reconcile(c,'one','day1')
    assert c.execute("SELECT payout_status FROM reconciliation WHERE transaction_id='TXN000001'").fetchone()[0]=='PAYOUT_MERCHANT_MISMATCH'
    c.close()


def test_payout_only_reference(tmp_path):
    c,_=_setup(tmp_path)
    c.execute("INSERT INTO payouts VALUES ('ORPHAN','M001',100,'EUR')")
    reconcile(c,'one','day1')
    assert c.execute("SELECT status FROM reconciliation WHERE transaction_id='ORPHAN'").fetchone()[0]=='PAYOUT_WITHOUT_PAYMENT'
    c.close()


def test_three_way_rerun_no_double_count(tmp_path):
    c,src=_setup(tmp_path)
    reconcile(c,'one','day1')
    first=c.execute('SELECT COUNT(*) FROM reconciliation').fetchone()[0]
    ingest(c,src/'payments.csv','payments')
    reconcile(c,'two','rerun')
    assert c.execute('SELECT COUNT(*) FROM reconciliation').fetchone()[0]==first
    assert c.execute('SELECT COUNT(*) FROM run_history').fetchone()[0]==2
    c.close()


def test_three_way_summary(tmp_path):
    result=run(tmp_path/'job',120)
    assert result['payout_status_counts']['PAYOUT_MATCHED'] > 0
    assert result['payout_status_counts']['PENDING_PAYOUT'] > 0
