if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer

from kkbox_poc.online_store import materialize_latest_features


@transformer
def push_features_to_redis(_, *args, **kwargs):
    return materialize_latest_features()
