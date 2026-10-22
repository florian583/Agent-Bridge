import test from 'node:test';
import assert from 'node:assert/strict';
import {parseArgs} from './bridge-availability.mjs';
test('unknown reset is omitted and exact model preserved',()=>{
 assert.deepEqual(parseArgs(['report','--agent','kimi','--model','exact','--reason','monthly_limit','--until','unknown']).params,{agent:'kimi',model:'exact',reason:'monthly_limit'});
});
test('known reset and clear preserve scope',()=>{
 assert.equal(parseArgs(['report','--agent','kimi','--reason','rate_limit','--until','2026-11-08T00:28:00+08:00']).params.resets_at,'2026-11-08T00:28:00+08:00');
 assert.deepEqual(parseArgs(['clear','--agent','kimi','--model','exact']).params,{agent:'kimi',model:'exact'});
});
test('invalid commands or ambiguous input fail before RPC',()=>{
 for(const args of [['oops'],['clear'],['report','--agent','kimi'],['list','--agent','kimi'],['check','--agent','kimi','--agent','codex'],['check','--agent']]) assert.throws(()=>parseArgs(args));
});

import net from 'node:net';
import fs from 'node:fs/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const exec=promisify(execFile);
test('shell report and clear reach shared RPC with exact scope',async()=>{
 const dir=await fs.mkdtemp('/tmp/ab-cli-'), socket=dir+'/s', requests=[];
 const server=net.createServer(client=>client.once('data',data=>{
  const request=JSON.parse(data);requests.push(request);
  client.end(JSON.stringify({ok:true,result:{accepted:true}})+'\n');
 }));
 await new Promise(resolve=>server.listen(socket,resolve));
 const env={...process.env,AGENT_BRIDGE_DAEMON_SOCKET:socket,BRIDGE_AVAILABILITY_CLI:new URL('./bridge-availability.mjs',import.meta.url).pathname};
 delete env.AGENT_BRIDGE_PARENT_CONTEXT;
 try {
  const script=new URL('./bridge-availability.sh',import.meta.url).pathname;
  await exec('/bin/sh',[script,'report','--agent','devin','--model','swe-2-high','--reason','rate_limit','--until','unknown'],{env});
  await exec('/bin/sh',[script,'clear','--agent','devin','--model','swe-2-high'],{env});
  assert.equal(requests[0].method,'report_usage_limit');
  assert.deepEqual(requests[0].params,{agent:'devin',model:'swe-2-high',reason:'rate_limit'});
  assert.equal(requests[1].method,'clear_usage_limit');
  await assert.rejects(exec('/bin/sh',[script,'clear','--agent','devin'],{env:{...env,AGENT_BRIDGE_PARENT_CONTEXT:'worker'}}));
  assert.equal(requests.length,2);
 } finally {await new Promise(resolve=>server.close(resolve));await fs.rm(dir,{recursive:true,force:true});}
});
