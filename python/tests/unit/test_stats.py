"""Tests for GlobalPipelineMetrics.progress_summary(), which lets wait loops
(Pipeline.wait_for_completion, testutils.wait_end_of_input) report whether a
long wait is stalled or just slow."""

from feldera.stats import CommitProgressSummary, GlobalPipelineMetrics

BASE_METRICS_DICT = {
    "state": "running",
    "incarnation_uuid": "00000000-0000-0000-0000-000000000000",
    "start_time": 0,
    "transaction_status": "NoTransaction",
    "total_processed_records": 42,
    "total_input_records": 100,
}


def test_commit_progress_absent_when_no_transaction():
    metrics = GlobalPipelineMetrics.from_dict(BASE_METRICS_DICT)
    assert metrics.commit_progress is None


def test_commit_progress_parsed_into_object():
    d = {
        **BASE_METRICS_DICT,
        "commit_progress": {
            "completed": 3,
            "in_progress": 2,
            "remaining": 1,
            "in_progress_processed_records": 10,
            "in_progress_total_records": 50,
        },
    }
    metrics = GlobalPipelineMetrics.from_dict(d)
    assert isinstance(metrics.commit_progress, CommitProgressSummary)
    assert metrics.commit_progress.completed == 3
    assert metrics.commit_progress.in_progress_total_records == 50


def test_commit_progress_str_matches_rust_display_format():
    progress = CommitProgressSummary.from_dict(
        {
            "completed": 3,
            "in_progress": 2,
            "remaining": 1,
            "in_progress_processed_records": 10,
            "in_progress_total_records": 50,
        }
    )
    assert str(progress) == (
        "completed: 3 operators, evaluating: 2 operators "
        "[10/50 changes processed], remaining: 1 operators"
    )


def test_progress_summary_falls_back_to_record_counts_outside_transaction():
    metrics = GlobalPipelineMetrics.from_dict(BASE_METRICS_DICT)
    assert metrics.progress_summary() == "42/100 records processed"


def test_progress_summary_prefers_commit_progress_during_transaction():
    d = {
        **BASE_METRICS_DICT,
        "commit_progress": {
            "completed": 0,
            "in_progress": 1,
            "remaining": 7,
            "in_progress_processed_records": 0,
            "in_progress_total_records": 169,
        },
    }
    metrics = GlobalPipelineMetrics.from_dict(d)
    summary = metrics.progress_summary()
    assert "0/169 changes processed" in summary
    assert "records processed" not in summary
