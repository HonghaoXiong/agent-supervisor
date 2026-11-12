#!/usr/bin/env python3
"""Send one reviewed turn through the fixed native desktop session."""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parent.parent/'lib'))
from zcode_supervisor.zcode_desktop_session_bridge import ZCodeDesktopSessionBridge
from zcode_supervisor.work_models import WorkError

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('session-id','delivery-id','contract','authorization'):
        parser.add_argument('--'+name,required=True)
    selection=parser.add_mutually_exclusive_group()
    selection.add_argument('--control-config')
    selection.add_argument('--channel')
    parser.add_argument('--channels-root',type=Path)
    parser.add_argument('--db-path',type=Path)
    parser.add_argument('--out')
    args=parser.parse_args()
    target=Path(args.out).absolute() if args.out else None
    if target is not None and target.exists():parser.error('Use a fresh output path for each attempt')
    contract=json.loads(Path(args.contract).read_text())
    authorization=json.loads(Path(args.authorization).read_text())
    model=contract.get('model')
    if isinstance(model,dict):model=model.get('modelId') or model.get('model_id')
    legacy_output=False
    try:
        bridge=ZCodeDesktopSessionBridge(control_config=args.control_config,channel_id=args.channel,
            channels_root=args.channels_root,db_path=args.db_path)
        config=bridge._config()
        legacy_output=not config.get('channelId')
        if config.get('channelId'):
            if target is not None:
                try:target.resolve().relative_to(Path(config['receiptDir']).resolve())
                except ValueError:
                    raise WorkError('channel-receipt-scope','Output must remain inside this channel receipt directory')
        elif target is None:
            raise WorkError('native-output-required','The legacy transport needs a fresh --out path')
        receipt=bridge.deliver(
            args.session_id,contract['prompt'],delivery_id=args.delivery_id,
            code_root=contract['workspace'],contract=contract,authorization=authorization,
            expected_model=model,attempt_id=args.delivery_id)
        code=0
    except WorkError as exc:
        receipt={'delivered':False,'error':exc.code,'message':exc.message};code=2
    if target is not None and code==0 and str(target)!=receipt.get('receipt_path'):
        with target.open('x') as handle:
            json.dump(receipt,handle,ensure_ascii=False,indent=2);handle.write('\n')
    elif target is not None and code!=0 and legacy_output:
        with target.open('x') as handle:
            json.dump(receipt,handle,ensure_ascii=False,indent=2);handle.write('\n')
    print(json.dumps(receipt,ensure_ascii=False,indent=2),flush=True)
    return code

if __name__=='__main__':raise SystemExit(main())
