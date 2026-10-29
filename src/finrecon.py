"""Deterministic financial reconciliation; all monetary values are integer euro cents."""
import argparse
import csv
import json
import random
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def generate(out: Path, count: int = 1200, seed: int = 27):
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    payments, settlements, payouts = [], [], []
    for i in range(count):
        ref = f'TXN{i:06d}'
        amount = rng.randint(500, 150000)
        merchant = f'M{rng.randrange(1, 81):03d}'
        payments.append(dict(transaction_id=ref, merchant_id=merchant, amount_cents=amount, currency='EUR', event_date='2026-10-20'))
        # deterministic, disjoint exception cohorts
        if i % 29 == 0:  # late settlement arrives in second feed
            continue
        if i % 31 == 0:  # absent settlement
            continue
        bank_amount = amount - 500 if i % 23 == 0 else amount
        settlements.append(dict(transaction_id=ref, amount_cents=bank_amount, currency='EUR', settled_date='2026-10-21'))
        payouts.append(dict(transaction_id=ref, merchant_id=merchant, payout_cents=amount - 100, currency='EUR'))
    # duplicate payment and unexpected bank settlement
    payments.append(payments[3].copy())
    settlements.append(dict(transaction_id='BANK_ONLY_001', amount_cents=7500, currency='EUR', settled_date='2026-10-21'))
    for name, rows in [('payments.csv', payments), ('settlements_day1.csv', settlements), ('payouts.csv', payouts)]:
        with (out / name).open('w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0])); writer.writeheader(); writer.writerows(rows)
    late = [dict(transaction_id=p['transaction_id'], amount_cents=p['amount_cents'], currency='EUR', settled_date='2026-10-23') for j,p in enumerate(payments[:count]) if j % 29 == 0]
    with (out/'settlements_late.csv').open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['transaction_id','amount_cents','currency','settled_date']); writer.writeheader(); writer.writerows(late)
    return {'payments_rows':len(payments),'settlements_day1_rows':len(settlements),'late_rows':len(late),'payout_rows':len(payouts)}


def connect(db: Path):
    db.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db)
    con.executescript('''
    CREATE TABLE IF NOT EXISTS payments (transaction_id TEXT PRIMARY KEY, merchant_id TEXT NOT NULL, amount_cents INTEGER NOT NULL, currency TEXT NOT NULL, event_date TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS settlements (transaction_id TEXT PRIMARY KEY, amount_cents INTEGER NOT NULL, currency TEXT NOT NULL, settled_date TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS payouts (transaction_id TEXT PRIMARY KEY, merchant_id TEXT NOT NULL, payout_cents INTEGER NOT NULL, currency TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS rejected (source TEXT, transaction_id TEXT, reason TEXT, PRIMARY KEY(source,transaction_id,reason));
    CREATE TABLE IF NOT EXISTS reconciliation (transaction_id TEXT PRIMARY KEY, merchant_id TEXT, payment_cents INTEGER, settled_cents INTEGER, payout_cents INTEGER, status TEXT NOT NULL, payout_status TEXT NOT NULL, discrepancy_cents INTEGER NOT NULL, payout_discrepancy_cents INTEGER NOT NULL, checked_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS run_history (run_id TEXT PRIMARY KEY, run_at TEXT NOT NULL, source_label TEXT NOT NULL, status_counts_json TEXT NOT NULL);
    ''')
    return con


def ingest(con, csv_path: Path, kind: str):
    definitions = {
      'payments':('transaction_id','merchant_id','amount_cents','currency','event_date'),
      'settlements':('transaction_id','amount_cents','currency','settled_date'),
      'payouts':('transaction_id','merchant_id','payout_cents','currency')}
    if kind not in definitions: raise ValueError('unknown source')
    cols = definitions[kind]
    accepted = rejected = 0
    with csv_path.open(newline='') as f:
        reader = csv.DictReader(f)
        if not set(cols).issubset(reader.fieldnames or []): raise ValueError(f'{kind}: missing required columns')
        for row in reader:
            ref = row['transaction_id'].strip()
            try:
                if not ref or row['currency'] != 'EUR': raise ValueError('invalid_reference_or_currency')
                value = int(row['amount_cents'] if kind != 'payouts' else row['payout_cents'])
                if value < 0: raise ValueError('negative_amount')
                vals = tuple(row[c] for c in cols)
                con.execute(f'INSERT INTO {kind} ({",".join(cols)}) VALUES ({",".join("?" for _ in cols)})',vals)
                accepted += 1
            except sqlite3.IntegrityError:
                con.execute('INSERT OR IGNORE INTO rejected VALUES (?,?,?)',(kind,ref,'duplicate_reference'))
                rejected += 1
            except (ValueError, TypeError) as exc:
                con.execute('INSERT OR IGNORE INTO rejected VALUES (?,?,?)',(kind,ref,str(exc)))
                rejected += 1
    con.commit()
    return {'accepted':accepted,'rejected':rejected}


def reconcile(con, run_id: str, source_label: str, merchant_fee_cents: int = 100):
    """Three-way reconciliation: bank must equal payment; payout must equal payment minus fixed synthetic fee.

    The flat 100-cent fee is a *demo contract*, not a general payment-network pricing model.
    The two dimensions are reported separately so a bank mismatch cannot hide a payout mismatch.
    """
    now = datetime.now(timezone.utc).isoformat()
    con.execute("""INSERT INTO reconciliation
        (transaction_id,merchant_id,payment_cents,settled_cents,payout_cents,status,payout_status,
         discrepancy_cents,payout_discrepancy_cents,checked_at)
      SELECT ids.transaction_id,p.merchant_id,p.amount_cents,s.amount_cents,o.payout_cents,
       CASE WHEN p.transaction_id IS NULL AND s.transaction_id IS NOT NULL THEN 'UNEXPECTED_SETTLEMENT'
            WHEN p.transaction_id IS NULL THEN 'PAYOUT_WITHOUT_PAYMENT'
            WHEN s.transaction_id IS NULL THEN 'PENDING_SETTLEMENT'
            WHEN p.currency != s.currency THEN 'CURRENCY_MISMATCH'
            WHEN p.amount_cents != s.amount_cents THEN 'AMOUNT_MISMATCH'
            ELSE 'MATCHED' END,
       CASE WHEN p.transaction_id IS NULL THEN 'PAYOUT_WITHOUT_PAYMENT'
            WHEN o.transaction_id IS NULL THEN 'PENDING_PAYOUT'
            WHEN p.merchant_id != o.merchant_id THEN 'PAYOUT_MERCHANT_MISMATCH'
            WHEN p.currency != o.currency THEN 'PAYOUT_CURRENCY_MISMATCH'
            WHEN p.amount_cents - ? != o.payout_cents THEN 'PAYOUT_AMOUNT_MISMATCH'
            ELSE 'PAYOUT_MATCHED' END,
       COALESCE(p.amount_cents,0)-COALESCE(s.amount_cents,0),
       CASE WHEN p.transaction_id IS NULL THEN 0 ELSE (p.amount_cents-?)-COALESCE(o.payout_cents,0) END, ?
      FROM (SELECT transaction_id FROM payments UNION SELECT transaction_id FROM settlements
            UNION SELECT transaction_id FROM payouts) ids
      LEFT JOIN payments p ON p.transaction_id=ids.transaction_id
      LEFT JOIN settlements s ON s.transaction_id=ids.transaction_id
      LEFT JOIN payouts o ON o.transaction_id=ids.transaction_id
      ON CONFLICT(transaction_id) DO UPDATE SET merchant_id=excluded.merchant_id,
      payment_cents=excluded.payment_cents,settled_cents=excluded.settled_cents,
      payout_cents=excluded.payout_cents,status=excluded.status,payout_status=excluded.payout_status,
      discrepancy_cents=excluded.discrepancy_cents,
      payout_discrepancy_cents=excluded.payout_discrepancy_cents,checked_at=excluded.checked_at""",
      (merchant_fee_cents,merchant_fee_cents,now))
    counts = dict(con.execute('SELECT status,COUNT(*) FROM reconciliation GROUP BY status').fetchall())
    payout_counts = dict(con.execute('SELECT payout_status,COUNT(*) FROM reconciliation GROUP BY payout_status').fetchall())
    con.execute('INSERT INTO run_history VALUES (?,?,?,?)',
                (run_id,now,source_label,json.dumps({'bank':counts,'payout':payout_counts},sort_keys=True)))
    con.commit()
    return counts


def export(con, out: Path):
    out.mkdir(parents=True,exist_ok=True)
    for table in ('reconciliation','rejected','run_history'):
        cur=con.execute(f'SELECT * FROM {table} ORDER BY 1')
        with (out/f'{table}.csv').open('w',newline='') as f:
            w=csv.writer(f);w.writerow([d[0] for d in cur.description]);w.writerows(cur)


def run(out: Path, count: int=1200):
    source=out/'data'; reports=out/'reports'; stats=generate(source,count)
    con=connect(out/'finrecon.sqlite')
    try:
        loads={}
        for kind,filename in [('payments','payments.csv'),('settlements','settlements_day1.csv'),('payouts','payouts.csv')]:
            loads[kind]=ingest(con,source/filename,kind)
        day1=reconcile(con,'day1','initial_bank_feed')
        # late settlement feed should change PENDING to MATCHED, no duplicate payments
        late_load=ingest(con,source/'settlements_late.csv','settlements')
        day2=reconcile(con,'day2','late_bank_feed')
        export(con,reports)
        result={'generated':stats,'ingestion':loads,'day1':day1,'late_ingestion':late_load,'day2':day2,
                'payout_status_counts':dict(con.execute('SELECT payout_status,COUNT(*) FROM reconciliation GROUP BY payout_status').fetchall()),
                'duplicate_rejections':con.execute("SELECT COUNT(*) FROM rejected WHERE reason='duplicate_reference'").fetchone()[0],
                'unique_payments':con.execute('SELECT COUNT(*) FROM payments').fetchone()[0]}
        (reports/'summary.json').write_text(json.dumps(result,indent=2))
        return result
    finally: con.close()

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,default=Path('output'));parser.add_argument('--count',type=int,default=1200)
    args=parser.parse_args();print(json.dumps(run(args.output,args.count),indent=2))
