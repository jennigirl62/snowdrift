from snowdrift.models import Column


def col(name, raw_type, platform="teradata", table="ORDERS", ordinal=1, schema="S", layer="final", **kw):
    return Column(platform=platform, schema=schema, table=table, name=name, ordinal=ordinal, raw_type=raw_type,
                  layer=layer, **kw)  # fmt: skip
