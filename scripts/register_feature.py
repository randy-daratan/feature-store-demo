from scripts.features import offline_feature_table, entity


def register_entity_and_features(client):
    client.apply(entity)
    client.apply(offline_feature_table)
