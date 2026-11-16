import re
from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models


def _ensure_reports_columns():
    """兼容已存在的卷：create_all 不会为旧表补列，这里幂等地补齐新增列并放开可空。

    reports 现在可以绑定 job 或方案运行 run_id 二者之一，因此 job_id 必须可空。
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    if 'reports' not in tables:
        return
    columns = {c['name']: c for c in inspector.get_columns('reports')}
    dialect = engine.dialect.name
    if 'run_id' not in columns:
        with engine.begin() as connection:
            connection.execute(text('ALTER TABLE reports ADD COLUMN run_id INTEGER NULL'))
    job_id = columns.get('job_id')
    if job_id and not job_id.get('nullable', True):
        if dialect == 'mysql':
            with engine.begin() as connection:
                connection.execute(text('ALTER TABLE reports MODIFY job_id INTEGER NULL'))
        elif dialect == 'sqlite':
            _rebuild_sqlite_reports()


def _rebuild_sqlite_reports():
    with engine.begin() as connection:
        ddl = connection.execute(text(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='reports'")).scalar()
        if not ddl:
            return
        new_ddl = re.sub(r'(?i)(\bjob_id\b\s+\w+(?:\(\d+\))?\s+)NOT\s+NULL', r'\1NULL', ddl, count=1)
        if new_ddl == ddl:
            return
        old_columns = [row[1] for row in connection.execute(text('PRAGMA table_info(reports)'))]
        connection.execute(text('ALTER TABLE reports RENAME TO reports_old'))
        connection.execute(text(new_ddl))
        # 显式列名复制：ADD COLUMN 追加的 run_id 与模型列顺序不同，SELECT * 会错位。
        shared = [c for c in old_columns if c != 'run_id']
        column_list = ', '.join(shared)
        connection.execute(text(
            f'INSERT INTO reports ({column_list}) SELECT {column_list} FROM reports_old'))
        connection.execute(text('DROP TABLE reports_old'))


def migrate():
    Base.metadata.create_all(engine)
    _ensure_reports_columns()


if __name__ == '__main__':
    migrate()
