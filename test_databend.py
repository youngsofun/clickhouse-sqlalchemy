from sqlalchemy import create_engine, Column, MetaData, text

from clickhouse_sqlalchemy import (
    Table, make_session, get_declarative_base, types, engines,
)

FORMAT_SUFFIX = 'FORMAT TabSeparatedWithNamesAndTypes'


def test_use(session):
    statement = "create database if not exists db2"
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")

    statement = text("use db2")
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")

    statement = "select database()"
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")
    return


def test_settings(session):
    statement = "select * from system.settings "
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")

    statement = text("set empty_as_default=0")
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")

    statement = "select * from system.settings "
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")
    return


def test_quote(session):
    statement = "select * from system.settings where name LIKE :st "
    v = list(session.execute(statement, {"st": "max%"}), )
    print(f"{statement} \n {v}")

    statement = "select * from system.settings where name = 'max_threads'"
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")

    statement = "select * from system.settings where name like 'max%'"
    v = list(session.execute(statement))
    print(f"{statement} \n {v}")


def main():
    uri = 'clickhouse+http://root:@localhost:8125/default'
    # uri = 'clickhouse+http://default:@localhost:8123/default'

    engine = create_engine(uri)
    session = make_session(engine)

    test_use(session)
    test_settings(session)
    test_quote(session)


if __name__ == "__main__":
    main()
