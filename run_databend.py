from sqlalchemy import create_engine, Column, MetaData, text

from clickhouse_sqlalchemy import (
    Table, make_session, get_declarative_base, types, engines,
)

FORMAT_SUFFIX = 'FORMAT TabSeparatedWithNamesAndTypes'

def run(session, statement):
    v = list(session.execute(text(statement)))
    print(f"{statement} \n {v}")


def test_use(s):
    run(s, "create database if not exists db2")
    run(s, "use db2")
    run(s, "select database()")


def test_settings(s):
    run(s, "select * from system.settings ")
    run(s, "set empty_as_default=0")
    run(s, "select * from system.settings ")


def test_quote(s):
    statement = "select * from system.settings where name LIKE :st "
    v = list(s.execute(statement, {"st": "max%"}), )
    print(f"{statement} \n {v}")

    run(s, "select * from system.settings where name = 'max_threads'")

    run(s, "select * from system.settings where name like 'max%'")


def test_semi(s):
    run(s, r"select parse_json('{\"k1\": [0, 1, 2]}'): k1[2]")


def main():
    uri = 'clickhouse+http://root:@localhost:8125/default'
    # uri = 'clickhouse+http://default:@localhost:8123/default'

    engine = create_engine(uri)
    session = make_session(engine)

    # test_use(session)
    # test_settings(session)
    # test_quote(session)
    test_semi(session)


if __name__ == "__main__":
    main()
