import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import fs from 'node:fs/promises';
import {availabilityCall} from './availability-client.mjs';

test('availability forwarding preserves structured unknown reset and scope', async()=>{
 const dir=await fs.mkdtemp('/tmp/ab-av-'), socket=dir+'/s';
 const server=net.createServer(client=>client.once('data',data=>{
  const request=JSON.parse(data.toString());
  assert.equal(request.method,'report_usage_limit');
  assert.equal(request.conversation_id,'conversation');
  assert.equal(request.params.resets_at,undefined);
  client.end(JSON.stringify({ok:true,result:{limit:{agent:'kimi',resets_at:null}}})+'\n');
 }));
 await new Promise(resolve=>server.listen(socket,resolve));
 try {assert.equal((await availabilityCall('report_usage_limit',{agent:'kimi',reason:'monthly_limit'},'conversation',{socket})).limit.resets_at,null);}
 finally {await new Promise(resolve=>server.close(resolve));await fs.rm(dir,{recursive:true,force:true});}
});

test('unavailable daemon never becomes a healthy empty list', async()=>{
 await assert.rejects(availabilityCall('check_availability',{},null,{socket:'/tmp/absent-availability-daemon.sock'}));
 await assert.rejects(availabilityCall('dispatch_task',{},null));
});

test('missing response is bounded and does not retry', async()=>{
 const dir=await fs.mkdtemp('/tmp/ab-av-'),socket=dir+'/s';let count=0;
 const server=net.createServer(client=>{count++;client.on('data',()=>{});});
 await new Promise(resolve=>server.listen(socket,resolve));
 try {await assert.rejects(availabilityCall('check_availability',{},null,{socket,timeoutMs:30}),/timed out/);assert.equal(count,1);}
 finally {await new Promise(resolve=>server.close(resolve));await fs.rm(dir,{recursive:true,force:true});}
});
