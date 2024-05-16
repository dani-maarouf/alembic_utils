# Alembic Utils

Fork of [alembic_utils](https://github.com/olirice/alembic_utils) with the following changes:

- Support ignoring schema (if e.g. `SET search_path TO x` is being used). This is configured by setting env `NEVER_INCLUDE_SCHEMA` to `true`
- Support rendering function definitions as multiline strings. This is configured by setting env `RENDER_DEF_MULTILINE` to `true`
- Support procedures. Procedures use `PgFunction`, but with `is_proc=True` passed to the constructor
- Function SQL is parsed and manipulated using a `libpg_query` [wrapper](https://github.com/dani-maarouf/psqlparse2), 
which should be more robust than the existing pattern matching parser

### Caveats

- Original function formatting and comments are lost when rendered in the migration script
- SQL constructs with equivalent representations may be transformed. e.g. `STRICT` may be changed to `RETURNS NULL ON NULL INPUT`
- Functions/procedures are the only entity types that use the new parser. Other entity types are still parsed using pattern matching

### TODO

- [ ] Use new parser for all entity types, not just functions

### Acknowledgements

- [Oliver Rice](https://github.com/olirice), author of the original `alembic_utils`
- [Diego Fulguiera](https://github.com/diegoful-fr) for adding initial support for ignoring schema prefix, and for procedures

---

https://github.com/olirice/alembic_utils

**Autogenerate Support for PostgreSQL Functions, Views, Materialized View, Triggers, and Policies**

[Alembic](https://alembic.sqlalchemy.org/en/latest/) is the defacto migration tool for use with [SQLAlchemy](https://www.sqlalchemy.org/). Without extensions, alembic can detect local changes to SQLAlchemy models and autogenerate a database migration or "revision" script. That revision can be applied to update the database's schema to match the SQLAlchemy model definitions.

Alembic Utils is an extension to alembic that adds support for autogenerating a larger number of [PostgreSQL](https://www.postgresql.org/) entity types, including [functions](https://www.postgresql.org/docs/current/sql-createfunction.html), [views](https://www.postgresql.org/docs/current/sql-createview.html), [materialized views](https://www.postgresql.org/docs/current/sql-creatematerializedview.html), [triggers](https://www.postgresql.org/docs/current/sql-createtrigger.html), and [policies](https://www.postgresql.org/docs/current/sql-createpolicy.html).

### TL;DR

Update alembic's `env.py` to register a function or view:

```python
# migrations/env.py
from alembic_utils.pg_function import PGFunction
from alembic_utils.replaceable_entity import register_entities


to_upper = PGFunction(
  schema='public',
  signature='to_upper(some_text text)',
  definition="""
  RETURNS text as
  $$
    SELECT upper(some_text)
  $$ language SQL;
  """
)

register_entities([to_upper])
```

You're done!

The next time you autogenerate a revision with
```shell
alembic revision --autogenerate -m 'create to_upper'
```
Alembic will detect if your entities are new, updated, or removed & populate the revison's `upgrade` and `downgrade` sections automatically.

For example:

```python
"""create to_upper

Revision ID: 8efi0da3a4
Revises:
Create Date: 2020-04-22 09:24:25.556995
"""
from alembic import op
import sqlalchemy as sa
from alembic_utils.pg_function import PGFunction

# revision identifiers, used by Alembic.
revision = '8efi0da3a4'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    public_to_upper_6fa0de = PGFunction(
        schema="public",
        signature="to_upper(some_text text)",
        definition="""
        returns text
        as
        $$ select upper(some_text) $$ language SQL;
        """
    )

    op.create_entity(public_to_upper_6fa0de)


def downgrade():
    public_to_upper_6fa0de = PGFunction(
        schema="public",
        signature="to_upper(some_text text)",
        definition="# Not Used"
    )

    op.drop_entity(public_to_upper_6fa0de)
```


Visit the [quickstart guide](https://olirice.github.io/alembic_utils/quickstart/) for usage instructions.

<p align="center">&mdash;&mdash;  &mdash;&mdash;</p>

### Contributing

To run the tests
```
# install pip dependencies
pip install wheel && pip install -e ".[dev]"

# run the tests
pytest src/test
```

To invoke the linter automated formatting and generally make use of precommit checks:
```
pip install pre-commit
pre-commit install

# manually run
pre-commit run --all
```
