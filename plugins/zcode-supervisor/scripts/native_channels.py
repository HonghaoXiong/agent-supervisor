#!/usr/bin/env python3
"""Register or list independent native desktop channels without model calls."""
import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'lib'))
from zcode_supervisor.native_channels import DEFAULT_ROOT, register_channel, list_channels
from zcode_supervisor.session_binding import session_status
from zcode_supervisor.work_models import WorkError


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=DEFAULT_ROOT)
    sub=parser.add_subparsers(dest='action',required=True)
    register=sub.add_parser('register')
    for name in ('channel','session-id','workspace'):register.add_argument('--'+name,required=True)
    register.add_argument('--owner-thread-id',default=os.environ.get('CODEX_THREAD_ID'))
    register.add_argument('--socket',type=Path)
    register.add_argument('--receipt-dir',type=Path)
    register.add_argument('--db-path',type=Path)
    sub.add_parser('list')
    args=parser.parse_args()
    try:
        if args.action=='list':result=list_channels(args.root)
        else:
            status=session_status(args.session_id,args.db_path)
            if status.get('status')!='located' or Path(status['session']['directory']).resolve()!=Path(args.workspace).resolve():
                raise WorkError('channel-session-mismatch','Session metadata does not match the requested workspace')
            register_channel(args.root,args.channel,session_id=args.session_id,workspace=args.workspace,
                owner_thread_id=args.owner_thread_id,socket_path=args.socket,receipt_dir=args.receipt_dir)
            result=next(row for row in list_channels(args.root) if row['channel_id']==args.channel)
        print(json.dumps(result,ensure_ascii=False,indent=2));return 0
    except WorkError as exc:
        print(json.dumps({'error':exc.code,'message':exc.message},ensure_ascii=False));return 2
    except (OSError,ValueError):
        print(json.dumps({'error':'channel-config-invalid','message':'Channel registration could not read its inputs'}));return 2

if __name__=='__main__':raise SystemExit(main())
