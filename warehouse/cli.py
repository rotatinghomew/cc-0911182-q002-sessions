import argparse
import json
from pathlib import Path

from . import sessions
from .export import export_tables
from .ingest import ingest
from .queries import overview
from .storage import open_store


def main():
    parser = argparse.ArgumentParser(description='本地仓库日常接收与核对')
    parser.add_argument('--database', default='warehouse.duckdb')
    commands = parser.add_subparsers(dest='action', required=True)
    load = commands.add_parser('load', help='接收一个业务包')
    load.add_argument('file', type=Path)
    commands.add_parser('overview', help='查看现有业务汇总')
    output = commands.add_parser('export', help='冻结列式导出')
    output.add_argument('destination', type=Path)
    output.add_argument('revision')
    sessions_parser = commands.add_parser('sessions', help='读取站点会话事实')
    sessions_parser.add_argument('site')
    sessions_parser.add_argument('--visitor')
    sessions_parser.add_argument('--start-day')
    rebuild = commands.add_parser('rebuild', help='从原始记录重建全部会话事实')
    rebuild.add_argument('--allow-restatement', action='store_true',
                         help='显式重述已冻结日期，并保留审计记录')
    days_parser = commands.add_parser('days', help='读取站点日集市')
    days_parser.add_argument('site')
    days_parser.add_argument('--day')
    member_parser = commands.add_parser('session', help='按会话引用读取成员明细')
    member_parser.add_argument('site')
    member_parser.add_argument('reference')
    disposition_parser = commands.add_parser('dispositions', help='查看事件去向')
    disposition_parser.add_argument('site')
    disposition_parser.add_argument('--status')
    restatement_parser = commands.add_parser('restatements', help='查看已发布日期的重述记录')
    restatement_parser.add_argument('site')
    reconcile_parser = commands.add_parser('reconcile', help='核对明细、去向与日集市')
    reconcile_parser.add_argument('site')
    args = parser.parse_args()
    connection = open_store(args.database)
    try:
        if args.action == 'load':
            result = ingest(connection, json.loads(args.file.read_text(encoding='utf-8')))
        elif args.action == 'export':
            result = export_tables(connection, args.destination, args.revision)
        elif args.action == 'sessions':
            result = sessions.read_sessions(connection, args.site, args.visitor, args.start_day)
        elif args.action == 'session':
            result = sessions.read_session(connection, args.site, args.reference)
        elif args.action == 'days':
            result = sessions.read_day_facts(connection, args.site, args.day)
        elif args.action == 'dispositions':
            result = sessions.read_dispositions(connection, args.site, args.status)
        elif args.action == 'restatements':
            result = sessions.read_restatements(connection, args.site)
        elif args.action == 'reconcile':
            result = sessions.reconcile(connection, args.site)
        elif args.action == 'rebuild':
            result = sessions.rebuild_facts(connection, allow_restatement=args.allow_restatement)
        else:
            result = overview(connection)
        print(json.dumps(result, ensure_ascii=False, default=str, indent=2))
    finally:
        connection.close()


if __name__ == '__main__':
    main()
