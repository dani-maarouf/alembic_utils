import logging
from uuid import uuid4

from parse import parse
from psqlparse2.pb.pg_query_pb2 import ParseResult
from psqlparse2 import query_to_pb, pb_to_query
import pg_query

from alembic_utils.exceptions import SQLParseFailure


logger = logging.getLogger(__name__)


def _get_create_function_stmt(parse_tree: ParseResult):
    if len(parse_tree.stmts) != 1:
        raise ValueError(f"Expected 1 statement, got {len(parse_tree.stmts)}")

    stmt = parse_tree.stmts[0]
    if not stmt.stmt.HasField("create_function_stmt") and not stmt.stmt.HasField("create_procedure_stmt"):
        raise ValueError(f"Expected create_function_stmt or create_procedure_stmt got {stmt}")

    create_func_stmt = (
        stmt.stmt.create_function_stmt
        if stmt.stmt.HasField("create_function_stmt")
        else stmt.stmt.create_procedure_stmt
    )
    return create_func_stmt


def _get_drop_stmt(parse_tree: ParseResult):
    if len(parse_tree.stmts) != 1:
        raise ValueError(f"Expected 1 statement, got {len(parse_tree.stmts)}")

    stmt = parse_tree.stmts[0]
    if not stmt.stmt.HasField("drop_stmt"):
        raise ValueError(f"Expected drop_stmt got {stmt}")
    return stmt.stmt.drop_stmt


def _get_func_signature_returns_and_type(func_sql: str):
    """
    return the function signature, returns clause, and whether it is a procedure
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("raw sql: %s", func_sql)
        parse_tree_json = pg_query.query_parse_json(func_sql)
        logger.debug("parse_tree_json: %s", parse_tree_json)

    parse_tree = query_to_pb(func_sql)
    create_func_stmt = _get_create_function_stmt(parse_tree)

    is_proc = create_func_stmt.is_procedure
    create_func_stmt.is_procedure = False
    create_func_stmt.replace = False
    create_func_stmt.ClearField("options")
    create_func_stmt.ClearField("sql_body")

    if len(create_func_stmt.funcname) > 1:
        del create_func_stmt.funcname[0]

    stmt_str = pb_to_query(parse_tree)

    prefix = "create function"
    if not stmt_str.lower().startswith(prefix):
        raise ValueError(f"Expected {prefix} got {stmt_str}")

    stmt_str = stmt_str[len(prefix) :].strip()
    return stmt_str, is_proc


def _get_func_schema(func_sql: str, default="public") -> str:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("raw sql: %s", func_sql)
        parse_tree_json = pg_query.query_parse_json(func_sql)
        logger.debug("parse_tree_json: %s", parse_tree_json)

    parse_tree = query_to_pb(func_sql)
    create_func_stmt = _get_create_function_stmt(parse_tree)
    if len(create_func_stmt.funcname) > 1:
        schema_name = create_func_stmt.funcname[0].string.sval
        if schema_name.startswith('"') or schema_name.startswith("'"):
            schema_name = schema_name[1:-1]
        return schema_name
    return default


def _get_func_body(func_sql: str) -> str:
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("raw sql: %s", func_sql)
        parse_tree_json = pg_query.query_parse_json(func_sql)
        logger.debug("parse_tree_json: %s", parse_tree_json)

    dummy_func_sql = "CREATE FUNCTION foo.bar() RETURNS void AS $$ BEGIN $$ LANGUAGE plpgsql;"
    parse_tree_dummy = query_to_pb(dummy_func_sql)
    dummy_create_func_stmt = _get_create_function_stmt(parse_tree_dummy)

    parse_tree = query_to_pb(func_sql)
    create_func_stmt = _get_create_function_stmt(parse_tree)
    create_func_stmt.is_procedure = False
    create_func_stmt.replace = False

    create_func_stmt.ClearField("funcname")
    for i in range(len(dummy_create_func_stmt.funcname)):
        create_func_stmt.funcname.add().CopyFrom(dummy_create_func_stmt.funcname[i])

    create_func_stmt.ClearField("parameters")
    for i in range(len(dummy_create_func_stmt.parameters)):
        create_func_stmt.parameters.add().CopyFrom(dummy_create_func_stmt.parameters[i])

    create_func_stmt.ClearField("return_type")
    create_func_stmt.return_type.CopyFrom(dummy_create_func_stmt.return_type)

    func_str = pb_to_query(parse_tree)
    func_split = func_str.split("RETURNS void")
    if len(func_split) != 2:
        raise ValueError(f"Expected 1 instance of RETURNS void in statement: {func_sql}")
    if func_split[0].strip().lower() != "create function foo.bar()":
        raise ValueError(f"Expected CREATE FUNCTION foo.bar() got {func_split[0]}")
    return func_split[1]


def validate_split_function(signature, returns, schema, body, is_proc):
    entity_kind = "PROCEDURE" if is_proc else "FUNCTION"
    reconstructed_sql = f"CREATE {entity_kind} {schema}.{signature} {returns} {body}"
    query_to_pb(reconstructed_sql)


def split_function(sql: str):
    """
    split a function or procedure into: signature, returns clause, schema, body, and whether it is a procedure
    """
    try:
        signature_and_return, is_proc = _get_func_signature_returns_and_type(sql)
        template = "{signature}returns{ret_type}"
        result = parse(template, signature_and_return, case_sensitive=False)
        if result is None:
            raw_signature = signature_and_return
            returns = ""
        else:
            raw_signature = result["signature"].strip()
            returns = f"returns {result['ret_type'].strip()}"

        schema = _get_func_schema(sql)
        body = _get_func_body(sql)

        # remove possible quotes from signature
        signature = "".join(raw_signature.split('"', 2)) if raw_signature.startswith('"') else raw_signature

        validate_split_function(signature, returns, schema, body, is_proc)
        return signature, returns, schema, body, is_proc
    except pg_query.PgQueryExc as e:
        raise SQLParseFailure(str(e)) from e


def render_drop_statement(func_sql: str, is_proc: bool) -> str:
    entity_kind = "PROCEDURE" if is_proc else "FUNCTION"

    dummy_drop_sql = f"DROP {entity_kind} sqrt(integer);"
    dummy_parse_tree = query_to_pb(dummy_drop_sql)
    drop_stmt = _get_drop_stmt(dummy_parse_tree)

    parse_tree = query_to_pb(func_sql)
    create_func_stmt = _get_create_function_stmt(parse_tree)

    assert len(drop_stmt.objects) == 1
    obj = drop_stmt.objects[0].object_with_args

    assert len(obj.objname) != 0

    obj.ClearField("objname")
    obj.ClearField("objargs")
    obj.ClearField("objfuncargs")

    for i in range(len(create_func_stmt.funcname)):
        obj.objname.append(create_func_stmt.funcname[i])

    for i in range(len(create_func_stmt.parameters)):
        p = create_func_stmt.parameters[i]
        p.function_parameter.ClearField("defexpr")
        obj.objfuncargs.append(p)

    return pb_to_query(dummy_parse_tree)


def normalize_whitespace(text, base_whitespace: str = " ") -> str:
    """Convert all whitespace to *base_whitespace*"""
    return base_whitespace.join(text.split()).strip()


def strip_terminating_semicolon(sql: str) -> str:
    """Removes terminating semicolon on a SQL statement if it exists"""
    return sql.strip().rstrip(";").strip()


def strip_double_quotes(sql: str) -> str:
    """Removes starting and ending double quotes"""
    sql = sql.strip().rstrip('"')
    return sql.strip().lstrip('"').strip()


def escape_colon_for_sql(sql: str) -> str:
    """Escapes colons for use in sqlalchemy.text"""
    holder = str(uuid4())
    sql = sql.replace("::", holder)
    sql = sql.replace(":", r"\:")
    sql = sql.replace(holder, "::")
    return sql


def escape_colon_for_plpgsql(sql: str) -> str:
    """Escapes colons for plpgsql for use in sqlalchemy.text"""
    holder1 = str(uuid4())
    holder2 = str(uuid4())
    holder3 = str(uuid4())
    sql = sql.replace("::", holder1)
    sql = sql.replace(":=", holder2)
    sql = sql.replace(r"\:", holder3)

    sql = sql.replace(":", r"\:")

    sql = sql.replace(holder3, r"\:")
    sql = sql.replace(holder2, ":=")
    sql = sql.replace(holder1, "::")
    return sql


def coerce_to_quoted(text: str) -> str:
    """Coerces schema and entity names to double quoted one

    Examples:
        coerce_to_quoted('"public"') => '"public"'
        coerce_to_quoted('public') => '"public"'
        coerce_to_quoted('public.table') => '"public"."table"'
        coerce_to_quoted('"public".table') => '"public"."table"'
        coerce_to_quoted('public."table"') => '"public"."table"'
    """
    if "." in text:
        schema, _, name = text.partition(".")
        schema = f'"{strip_double_quotes(schema)}"'
        name = f'"{strip_double_quotes(name)}"'
        return f"{schema}.{name}"

    text = strip_double_quotes(text)
    return f'"{text}"'


def coerce_to_unquoted(text: str) -> str:
    """Coerces schema and entity names to unquoted

    Examples:
        coerce_to_unquoted('"public"') => 'public'
        coerce_to_unquoted('public') => 'public'
        coerce_to_unquoted('public.table') => 'public.table'
        coerce_to_unquoted('"public".table') => 'public.table'
    """
    return "".join(text.split('"'))
