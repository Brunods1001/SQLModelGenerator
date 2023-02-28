"A module to create SQLModels from a db connection"
import black
from black.mode import Mode
import keyword
from re import sub as re_sub
from sqlalchemy import Column, MetaData

from pype.db import connect

TAB = " " * 4

dbconn = connect()
print(dbconn, dbconn.connection_ok())


def camelcase(txt: str) -> str:
    return re_sub(r"(_|-)+", " ", txt).title().replace(" ", "")


def snakecase(txt: str) -> str:
    return "_".join(
        re_sub(
            "([A-Z][a-z]+)", r" \1", re_sub("([A-Z]+)", r" \1", txt.replace("-", " "))
        ).split()
    ).lower()


def create_fk(col: Column) -> str:
    if len(col.foreign_keys) > 1:
        raise Exception("More than one foreign key found!")
    elif len(col.foreign_keys) == 0:
        raise Exception("No foreign key found!")
    res = list(col.foreign_keys)[0]
    fk: str | None = None
    if res is not None:
        if res.column is not None:
            fk = res.column.name.split(".")[-1]
    if fk is None:
        raise Exception("Couldn't get foreign key")
    schema_tablename: str = col.table
    res = f'"{schema_tablename}.{fk}"'
    print("Foreign key", res)
    return res


def create_field(col: Column) -> str:
    type_ = col.type.python_type.__name__
    if col.nullable or col.primary_key:
        type_ = f"None | {type_}"

    alias = f"\"{col.name}\""
    if col.name in keyword.kwlist:
        varname = f"{col.name}_: {type_}"
    else:
        varname = f"{col.name}: {type_}"
    default = "None"
    is_pk = col.primary_key
    if col.foreign_keys:
        fk = create_fk(col)
    else:
        fk = "None"
    vardef = f"Field(alias={alias}, default={default}, primary_key={is_pk}, foreign_key={fk})"

    if vardef:
        field = f"{varname} = {vardef}"
    else:
        field = varname

    return field


def generate_pk() -> str:
    return "id: None | int = Field(default=None, primary_key=True)"


def write_models(path: str, schemas: list[str] = []):
    page = """from datetime import date, datetime
from decimal import Decimal
from sqlmodel import Field, SQLModel\n\n\n"""
    for schema in schemas:
        meta = MetaData(schema=schema)
        meta.reflect(bind=dbconn.engine)

        for table in meta.sorted_tables:
            haspk = False
            tablename = camelcase(table.name)
            tablename_schema = camelcase(table.name) + camelcase(schema)
            model_txt = f"class {tablename_schema}(SQLModel, table=True):\n"
            model_txt += TAB + f'__table_args__ = {{"schema": "{schema}"}}\n'
            model_txt += TAB + f'__tablename__: str ="{snakecase(tablename)}"\n\n'
            for col in table.columns:
                field = create_field(col)
                haspk = haspk or col.primary_key

                model_txt += TAB + field + "\n"
            if not haspk:
                model_txt += TAB + generate_pk() + "\n"
            page += model_txt + "\n\n"

    with open("raw_models.py", "w") as f:
        f.write(page)

    page = black.format_str(page, mode=Mode())

    with open(path, "w") as f:
        f.write(page)

