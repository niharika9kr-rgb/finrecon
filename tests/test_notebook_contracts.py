from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "notebooks"

def test_ingest_quarantines_duplicate_ids():
    text = (ROOT / "01_ingest_delta.py").read_text()
    assert "DUPLICATE_TRANSACTION_ID" in text
    assert "quarantine_{name}" in text
    assert "dropDuplicates" not in text

def test_reconcile_rejects_duplicate_keys():
    text = (ROOT / "02_reconcile_delta.py").read_text()
    assert "duplicate IDs found" in text
    assert '"full_outer"' in text
    assert "PAYOUT_AMOUNT_MISMATCH" in text

def test_late_batch_quarantines_conflicts():
    text = (ROOT / "03_late_settlement_recovery.py").read_text()
    assert "CONFLICT_WITH_EXISTING" in text
    assert "DUPLICATE_IN_LATE_BATCH" in text
    assert "whenNotMatchedInsertAll" in text
