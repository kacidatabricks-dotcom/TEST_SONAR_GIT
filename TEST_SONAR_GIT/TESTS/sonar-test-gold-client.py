
    
from unittest.mock import MagicMock

import importlib
save_dim_clients_to_gold = importlib.import_module("sonar-fonc-gold-client").save_dim_clients_to_gold


def test_save_dim_client_to_gold():

    # Préparation
    df = MagicMock()

    # Exécution
    save_dim_clients_to_gold(
        df,
        "gold.test_dim_clients"
    )

    # Vérifications
    df.write.format.assert_called_once_with("delta")

    df.write.format.return_value.mode.assert_called_once_with(
        "overwrite"
    )

    (
        df.write.format.return_value
        .mode.return_value
        .option.assert_called_once_with(
            "overwriteSchema", "true"
        )
    )

    (
        df.write.format.return_value
        .mode.return_value
        .option.return_value
        .saveAsTable.assert_called_once_with(
            "gold.test_dim_clients"
        )
    )
