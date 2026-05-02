if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer

from kkbox_poc.features import build_feature_snapshot_from_frames, persist_feature_snapshot


@transformer
def build_kkbox_features(raw_tables, *args, **kwargs):
    feature_frame, snapshot_date = build_feature_snapshot_from_frames(
        train=raw_tables["train"],
        members=raw_tables["members"],
        transactions=raw_tables["transactions"],
        user_logs=raw_tables["user_logs"],
        reference_date=raw_tables["reference_date"],
    )
    row_count = persist_feature_snapshot(feature_frame)
    return {"rows": row_count, "snapshot_date": str(snapshot_date)}
