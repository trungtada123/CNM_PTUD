if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader

from kkbox_poc.features import load_raw_tables, resolve_reference_date


@data_loader
def load_kkbox_raw(*args, **kwargs):
    raw_tables = load_raw_tables()
    raw_tables["reference_date"] = resolve_reference_date(raw_tables["transactions"], raw_tables["user_logs"])
    return raw_tables
