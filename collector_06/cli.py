import argparse
import json
from .store import RecordStore
from .capture import capture
from .engine import load_scenario, run_scenario


def main(argv=None):
    parser = argparse.ArgumentParser(description='直播带岗活动的会话与回放采集')
    parser.add_argument("--database", default="collector.sqlite3")
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--capture", metavar="FIXTURE_ROOT")
    parser.add_argument("--route", default='/schedule')
    parser.add_argument("--observed-at", default="2026-11-11T10:00:00Z")
    parser.add_argument("--run", metavar="SCENARIO", help="离线场景文件，重建日程、场次版本与回放归属")
    parser.add_argument("--output", metavar="RESULT", help="--run 的业务结果输出文件（JSON）")
    args = parser.parse_args(argv)
    if args.capture:
        print(json.dumps(capture(args.capture, args.route, args.database, args.observed_at), ensure_ascii=False))
        return 0
    if args.run:
        scenario = load_scenario(args.run)
        result = run_scenario(scenario, args.database)
        rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as handle:
                handle.write(rendered)
        print(rendered)
        return 0
    if args.init:
        store = RecordStore(args.database)
        store.initialize()
        print(json.dumps({"stored": store.count()}))
        return 0
    parser.print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
