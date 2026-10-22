"""Add availability forwarding to the existing user-level Bridge Live relay."""
import shutil
from pathlib import Path

root = Path.home() / '.claude-main/skills/bridge-live'
relay = root / 'relay.mjs'
source = relay.read_text()
if "from './availability-client.mjs'" not in source:
    anchors = ['server.setRequestHandler(ListToolsRequestSchema,async()=>({tools:[', '    switch(req.params.name) {']
    if any(source.count(anchor) != 1 for anchor in anchors):
        raise SystemExit('Bridge Live relay changed; inspect before updating')
    shutil.copy2(relay, root / 'relay.mjs.before-availability-20261022')
    source = "import {availabilityTools,availabilityCall} from './availability-client.mjs';\n" + source
    source = source.replace(anchors[0], anchors[0] + '\n  ...availabilityTools,')
    source = source.replace(anchors[1], anchors[1] + "\n      case 'check_availability':\n      case 'report_usage_limit':\n      case 'clear_usage_limit':\n        return result({ok:true,...await availabilityCall(req.params.name,a,session)});")
    relay.write_text(source)
shutil.copy2(Path(__file__).with_name('availability-client.mjs'), root / 'availability-client.mjs')
print('Bridge Live availability tools installed; existing connections load them on reconnect')

for name in ('bridge-availability.mjs',):
    shutil.copy2(Path(__file__).with_name(name), root / name)
bin_dir = Path.home() / '.local/bin'
bin_dir.mkdir(parents=True, exist_ok=True)
cli = bin_dir / 'bridge-availability.sh'
shutil.copy2(Path(__file__).with_name('bridge-availability.sh'), cli)
cli.chmod(0o755)
print(f'Shell fallback ready now: {cli}')
