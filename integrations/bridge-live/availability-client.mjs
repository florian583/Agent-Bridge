import net from 'node:net';
import os from 'node:os';
import path from 'node:path';

const methods = new Set(['check_availability', 'report_usage_limit', 'clear_usage_limit', 'list_agents']);
const str = {type:'string'};
const shape = (properties, required=[]) => ({type:'object',properties,required,additionalProperties:false});
export const availabilityTools = [
  {name:'check_availability',description:'Check engine/model quota, retained usage limits and active capacity before dispatch. Unknown is not exhausted. Uses the same Agent Bridge daemon; no worker work is started.',inputSchema:shape({agent:str,model:str})},
  {name:'report_usage_limit',description:'Record a user/provider-reported engine or exact-model limit. reason: monthly_limit, usage_limit, rate_limit or operator_hold. resets_at: ISO timestamp with timezone; omit if unknown (explicit clear then required). Does not cancel running work.',inputSchema:shape({agent:str,model:str,reason:{enum:['monthly_limit','usage_limit','rate_limit','operator_hold']},resets_at:str},['agent','reason'])},
  {name:'clear_usage_limit',description:'Clear an exact engine/model limit after verified recovery or user correction. Does not override provider quota. Recheck availability before dispatch.',inputSchema:shape({agent:str,model:str},['agent'])},
];

export function availabilityCall(method, params, conversation, options={}) {
  if (!methods.has(method)) return Promise.reject(new Error('Unsupported availability method'));
  const nested = process.env.AGENT_BRIDGE_PARENT_CONTEXT === 'worker';
  if (nested && !['check_availability', 'list_agents'].includes(method)) return Promise.reject(new Error('Nested worker mutation disabled'));
  const socketPath = options.socket || process.env.AGENT_BRIDGE_DAEMON_SOCKET || path.join(os.homedir(),'.agent-bridge-daemon/daemon.sock');
  const payload = JSON.stringify({method,params,conversation_id:conversation,client_pid:process.pid,runtime_context:nested?'worker':'coordinator'})+'\n';
  if (Buffer.byteLength(payload)>65536) return Promise.reject(new Error('Availability request too large'));
  return new Promise((resolve,reject)=>{
    const socket=net.createConnection(socketPath);
    let done=false, data=Buffer.alloc(0);
    const finish=(error,value)=>{if(done)return;done=true;clearTimeout(timer);socket.destroy();error?reject(error):resolve(value);};
    const timer=setTimeout(()=>finish(new Error('Availability check timed out')),options.timeoutMs || 30000);
    socket.on('connect',()=>socket.write(payload));
    socket.on('error',error=>finish(error));
    socket.on('end',()=>{if(!done)finish(new Error('Availability response missing'));});
    socket.on('data',chunk=>{
      if(data.length+chunk.length>2*1024*1024)return finish(new Error('Availability response too large'));
      data=Buffer.concat([data,chunk]);
      const end=data.indexOf(10);if(end<0)return;
      try {
        const reply=JSON.parse(data.subarray(0,end).toString());
        if(reply.ok!==true)throw new Error(reply.error || 'Availability request failed');
        finish(null,reply.result);
      } catch(error){finish(error);}
    });
  });
}
