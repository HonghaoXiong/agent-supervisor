import net from 'node:net';
import fs from 'node:fs/promises';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';

const hash = text => crypto.createHash('sha256').update(text).digest('hex');
const json = value => JSON.stringify(value)+'\n';
const failure = code => ({ok:false,error:code});
const MAX_BYTES = 1024*1024;
let active;

async function privateDir(directory) {
  await fs.mkdir(directory,{recursive:true,mode:0o700});
  const stat=await fs.lstat(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid!==process.getuid())
    throw new Error('unsafe-control-directory');
  await fs.chmod(directory,0o700);
}

async function writeRecord(file,value) {
  const temp=file+'.'+crypto.randomUUID()+'.tmp';
  await createRecord(temp,value);
  await fs.rename(temp,file);
  await syncDir(path.dirname(file));
}

async function syncDir(directory) {
  const handle=await fs.open(directory,'r');
  try{await handle.sync();}finally{await handle.close();}
}

async function createRecord(file,value) {
  const handle=await fs.open(file,'wx',0o600);
  try{await handle.writeFile(json(value));await handle.sync();}
  finally{await handle.close();}
  await syncDir(path.dirname(file));
}

async function acquireOwner(socketPath) {
  const lock=socketPath+'.owner';const nonce=crypto.randomUUID();let recovered=false;
  try{await createRecord(lock,{pid:process.pid,nonce});}
  catch(error){
    if(error.code!=='EEXIST')throw error;
    const stat=await fs.lstat(lock);
    if(!stat.isFile()||stat.uid!==process.getuid()||(stat.mode&0o077))throw new Error('unsafe-owner');
    const old=JSON.parse(await fs.readFile(lock,'utf8'));
    if(!Number.isSafeInteger(old.pid)||old.pid<=0)throw new Error('unknown-owner');
    try{process.kill(old.pid,0);throw new Error('control-already-running');}
    catch(check){if(check.code!=='ESRCH')throw check;}
    if((await fs.lstat(lock)).ino!==stat.ino)throw new Error('owner-changed');
    await fs.unlink(lock);await createRecord(lock,{pid:process.pid,nonce});recovered=true;
  }
  const release=async()=>{
    const current=JSON.parse(await fs.readFile(lock,'utf8'));
    if(current.nonce===nonce){await fs.unlink(lock);await syncDir(path.dirname(lock));}
  };
  try{
    let info;try{info=await fs.lstat(socketPath);}catch(error){if(error.code!=='ENOENT')throw error;}
    if(info){
      if(!recovered||!info.isSocket()||info.uid!==process.getuid()||(info.mode&0o077))
        throw new Error('existing-control-endpoint');
      const dead=await new Promise(resolve=>{
        const probe=net.createConnection(socketPath);
        probe.setTimeout(1000,()=>{probe.destroy();resolve(false);});
        probe.on('connect',()=>{probe.destroy();resolve(false);});
        probe.on('error',error=>resolve(error.code==='ECONNREFUSED'));
      });
      if(!dead||(await fs.lstat(socketPath)).ino!==info.ino)throw new Error('endpoint-not-proven-dead');
      await fs.unlink(socketPath);
    }
    return release;
  }catch(error){await release();throw error;}
}

export function validateControlConfig(config) {
  if (!config || !/^[a-f0-9]{64}$/.test(config.token??'') ||
      !/^sess_[A-Za-z0-9_-]+$/.test(config.sessionId??'') ||
      ![config.socket,config.stateDir,config.workspace].every(x=>typeof x==='string'&&path.isAbsolute(x)))
    throw new Error('invalid-control-config');
  if(config.channelId!==undefined && (
    typeof config.channelId!=='string' || !/^[a-z0-9][a-z0-9_-]{0,47}$/.test(config.channelId) ||
    config.channelId==='legacy' || typeof config.ownerThreadId!=='string' ||
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(config.ownerThreadId) ||
    typeof config.receiptDir!=='string' || !path.isAbsolute(config.receiptDir)))
    throw new Error('invalid-control-config');
}

export async function startControl(ctx,config) {
  validateControlConfig(config);
  config={...config};
  const scope=config.channelId===undefined?{}:{channel_id:config.channelId,owner_thread_id:config.ownerThreadId};
  const scopedFailure=code=>({...failure(code),...scope});
  await privateDir(path.dirname(config.socket));
  await privateDir(config.stateDir);
  const releaseOwner=await acquireOwner(config.socket);
  const inFlight=new Map();
  const sockets=new Set();
  let closePromise;

  async function dispatch(request) {
    const presented=Buffer.from(typeof request?.token==='string'?request.token:'');
    const wanted=Buffer.from(config.token);
    if (presented.length!==wanted.length || !crypto.timingSafeEqual(presented,wanted))
      return failure('unauthorized');
    if(config.channelId!==undefined &&
        (request.channel_id!==config.channelId || request.owner_thread_id!==config.ownerThreadId))
      return failure('scope-conflict');
    if (request.op==='status') return {ok:true,channel:'zcode-native-extension',...scope,session_id:config.sessionId};
    if (request.op==='request_grants') {
      const result=await ctx.grants.request();
      await writeRecord(path.join(path.dirname(config.stateDir),'grant-result.json'),result);
      return {ok:result?.granted?.includes('sessions.write')===true,...scope,granted:result?.granted??[]};
    }
    if (request.op!=='send' || request.session_id!==config.sessionId)
      return scopedFailure('scope-conflict');
    if (!/^[A-Za-z0-9_-]{1,100}$/.test(request.delivery_id??'') ||
        typeof request.prompt!=='string' || !request.prompt.trim() ||
        request.prompt.length>1000000 || request.body_sha256!==hash(request.prompt) ||
        !/^[a-f0-9]{64}$/.test(request.contract_sha256??''))
      return scopedFailure('invalid-delivery');
    const key=request.delivery_id;
    const file=path.join(config.stateDir,key+'.json');
    const matches=record=>record.session_id===request.session_id &&
      (config.channelId===undefined ||
        (record.channel_id===config.channelId && record.owner_thread_id===config.ownerThreadId)) &&
      record.body_sha256===request.body_sha256 && record.contract_sha256===request.contract_sha256;
    if (inFlight.has(key)) {
      const result=await inFlight.get(key);
      const record=JSON.parse(await fs.readFile(file,'utf8'));
      return matches(record)?result:scopedFailure('delivery-id-conflict');
    }
    const work=(async()=>{
      let old;
      try {old=JSON.parse(await fs.readFile(file,'utf8'));}
      catch(error) {if(error.code!=='ENOENT')throw error;}
      if (old) {
        if (!matches(old))return scopedFailure('delivery-id-conflict');
        if (old.response)return old.response;
        return scopedFailure('delivery-unconfirmed');
      }
      const intent={...scope,session_id:request.session_id,body_sha256:request.body_sha256,
        contract_sha256:request.contract_sha256,request_id:crypto.randomUUID(),
        state:'pending',created_at:new Date().toISOString()};
      await createRecord(file,intent);
      let response;
      try {
        const result=await ctx.sessions.sendToSession({sessionId:request.session_id,
          prompt:request.prompt,requestId:intent.request_id});
        const operationSession=result?.operation?.sessionId;
        if (operationSession && operationSession!==request.session_id)
          throw new Error('native-session-mismatch');
        response={ok:result?.admission==='accepted',...scope,request_id:intent.request_id,
          session_id:request.session_id,admission:result?.admission??'unknown',
          delivery:result?.delivery??'unknown',reason:result?.reason};
        intent.state=response.ok?'accepted':(result?.admission==='rejected'?'rejected':'unknown');
      } catch(error) {
        intent.native_error={name:String(error?.name??'Error').slice(0,100),
          code:String(error?.code??'').slice(0,100),message:String(error?.message??'').slice(0,500)};
        intent.state='unknown';response=scopedFailure('native-delivery-unconfirmed');
      }
      intent.response=response;
      await writeRecord(file,intent);
      return response;
    })();
    inFlight.set(key,work);
    try {return await work;} finally {inFlight.delete(key);}
  }

  const server=net.createServer(socket=>{
    sockets.add(socket);let bytes=0;let buffer='';let consumed=false;
    socket.setEncoding('utf8');socket.setTimeout(30000,()=>socket.destroy());
    socket.on('error',()=>{});socket.on('close',()=>sockets.delete(socket));
    socket.on('data',chunk=>{
      if(consumed)return;
      bytes+=Buffer.byteLength(chunk);buffer+=chunk;
      if(bytes>MAX_BYTES){consumed=true;socket.end(json(failure('request-too-large')));return;}
      const end=buffer.indexOf('\n');if(end<0)return;consumed=true;
      let request;try{request=JSON.parse(buffer.slice(0,end));}
      catch{socket.end(json(failure('invalid-json')));return;}
      Promise.resolve(dispatch(request)).then(result=>socket.end(json(result)),
        ()=>socket.end(json(failure('control-error'))));
    });
  });
  try{await new Promise((resolve,reject)=>{
    server.once('error',reject);server.listen(config.socket,()=>{
      server.removeListener('error',reject);resolve();
    });
  });}catch(error){await releaseOwner();throw error;}
  server.on('error',()=>{});
  await fs.chmod(config.socket,0o600);
  return {close:()=>{
    if(closePromise)return closePromise;
    for(const socket of sockets)socket.destroy();
    closePromise=(async()=>{
      await new Promise(resolve=>server.close(resolve));
      await Promise.allSettled([...inFlight.values()]);
      await releaseOwner();
    })();
    return closePromise;
  }};
}

export async function activate(ctx) {
  if(active)return;
  const {startChannelManager}=await import('./channel-manager.mjs');
  active=await startChannelManager(ctx,{directory:path.join(os.homedir(),'.czs','native-gui')});
}

export async function deactivate() {
  if(active){await active.close();active=undefined;}
}
