"""Prostředí Alembicu.

Migrace spouští server sám (při startu nebo `meshcore-server db migrate`)
a předává hotové spojení v config.attributes["connection"].
Nové migrace pro vývojáře: viz README, sekce Vývoj.
"""

from alembic import context
from sqlalchemy import create_engine

from meshcore_server.db.schema import metadata

config = context.config


def _run(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=metadata,
        render_as_batch=True,  # SQLite neumí většinu ALTER TABLE
        compare_type=True,
        user_module_prefix="meshcore_server.db.types.",
    )
    with context.begin_transaction():
        context.run_migrations()


connection = config.attributes.get("connection")
if connection is not None:
    _run(connection)
else:
    engine = create_engine(config.get_main_option("sqlalchemy.url"))
    with engine.begin() as conn:
        _run(conn)
    engine.dispose()
