from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import Insert, Update

"""
When imported, automatically make all insert not fail on duplicate keys
"""


@compiles(Insert, "mysql")
def mysql_insert_ignore(insert, compiler, **kw):
    return compiler.visit_insert(insert.prefix_with("IGNORE"), **kw)

@compiles(Update, "mysql")
def mysql_update_ignore(update, compiler, **kw):
    return compiler.visit_update(update.prefix_with("IGNORE"), **kw)

