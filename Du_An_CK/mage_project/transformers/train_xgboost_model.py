if "transformer" not in globals():
    from mage_ai.data_preparation.decorators import transformer

from kkbox_poc.training import train_and_register_model


@transformer
def train_xgboost_model(_, *args, **kwargs):
    return train_and_register_model()
