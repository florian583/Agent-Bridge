import {availabilityCall} from './availability-client.mjs';

const help = `Usage: bridge-availability.sh COMMAND [OPTIONS]
  list                         Engine availability, retained model limits, preferences
  check --agent NAME [--model ID]
  models                       Engine inventory and model-selection guidance
  priorities                   Current coordinator routing preferences
  report --agent NAME --reason rate_limit|usage_limit|monthly_limit|operator_hold
         [--model ID] [--until ISO_TIMESTAMP_WITH_TIMEZONE|unknown]
  clear --agent NAME [--model ID]

JSON output. Unknown reset remains blocked until explicitly cleared.
Model IDs are provider-specific; check an exact ID before dispatch.
Uses the live daemon, without reconnecting Claude or starting workers.
AGENT_BRIDGE_DAEMON_SOCKET overrides the Unix socket. Errors exit 1.
`;

export function parseArgs(args) {
  const [command='help', ...rest] = args;
  if (['help','--help','-h'].includes(command)) return {help:true};
  const methods = {list:'check_availability',check:'check_availability',models:'list_agents',priorities:'check_availability',report:'report_usage_limit',clear:'clear_usage_limit'};
  if (!Object.hasOwn(methods,command)) throw new Error('Unknown command: '+command);
  const allowed = {list:[],check:['agent','model'],models:[],priorities:[],report:['agent','model','reason','until'],clear:['agent','model']}[command];
  const options = {};
  for (let i=0;i<rest.length;i+=2) {
    const key=rest[i].slice(2), value=rest[i+1];
    if (!rest[i].startsWith('--') || !allowed.includes(key) || Object.hasOwn(options,key) || !value || value.startsWith('--')) throw new Error('Invalid or duplicate option: '+rest[i]);
    options[key]=value;
  }
  if (['check','report','clear'].includes(command) && !options.agent) throw new Error('--agent is required');
  if (command==='report' && !['rate_limit','usage_limit','monthly_limit','operator_hold'].includes(options.reason)) throw new Error('A valid --reason is required');
  if (options.until && options.until!=='unknown') options.resets_at=options.until;
  delete options.until;
  return {command,method:methods[command],params:options};
}

export async function main(args) {
  const parsed=parseArgs(args);
  if (parsed.help) {console.log(help);return;}
  const result=await availabilityCall(parsed.method,parsed.params,null);
  const output=parsed.command==='models' ? {inventory:result, ...await availabilityCall('check_availability',{},null), model_catalogue:'Inventory includes provider model-selection guidance, not an exhaustive live model catalogue. Check exact model IDs explicitly.'} : parsed.command==='priorities'?result.coordinator:result;
  console.log(JSON.stringify(output,null,2));
}

if (import.meta.url === new URL(process.argv[1], 'file:').href) {
  main(process.argv.slice(2)).catch(error=>{console.error('bridge-availability: '+error.message);process.exitCode=1;});
}
