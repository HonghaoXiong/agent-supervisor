import fs from 'node:fs/promises';
import {constants} from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import crypto from 'node:crypto';
import {startControl,validateControlConfig} from './native-control.mjs';

const MAX_CONFIG_BYTES=1024*1024;
const validId=id=>/^[a-z0-9][a-z0-9_-]{0,47}$/.test(id)&&id!=='legacy';
const hash=raw=>crypto.createHash('sha256').update(raw).digest('hex');
const json=value=>JSON.stringify(value)+'\n';

async function privateDirectory(directory) {
  const stat=await fs.lstat(directory);
  if(!stat.isDirectory()||stat.isSymbolicLink()||stat.uid!==process.getuid()||(stat.mode&0o077))
    throw new Error('unsafe-directory');
}

async function readConfig(file) {
  if(typeof file!=='string'||!path.isAbsolute(file))throw new Error('invalid-config-path');
  await privateDirectory(path.dirname(file));
  const before=await fs.lstat(file);
  if(!before.isFile()||before.isSymbolicLink()||before.uid!==process.getuid()||(before.mode&0o077))
    throw new Error('unsafe-config');
  const handle=await fs.open(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try {
    const stat=await handle.stat();
    if(stat.ino!==before.ino||stat.dev!==before.dev)throw new Error('config-changed-during-read');
    if(stat.size>MAX_CONFIG_BYTES)throw new Error('config-too-large');
    const buffer=Buffer.alloc(MAX_CONFIG_BYTES+1);let length=0;
    while(length<buffer.length){
      const {bytesRead}=await handle.read(buffer,length,buffer.length-length,null);
      if(!bytesRead)break;length+=bytesRead;
    }
    if(length>MAX_CONFIG_BYTES)throw new Error('config-too-large');
    const raw=buffer.subarray(0,length).toString('utf8');
    return {value:JSON.parse(raw),digest:hash(raw)};
  } finally {await handle.close();}
}

async function canonical(location) {
  try{return await fs.realpath(location);}
  catch(error){
    if(error.code!=='ENOENT')throw error;
    const parent=path.dirname(location);
    if(parent===location)throw error;
    return path.join(await canonical(parent),path.basename(location));
  }
}
const overlaps=(a,b)=>a===b||a.startsWith(b+path.sep)||b.startsWith(a+path.sep);

async function writeRecord(file,value) {
  const temp=file+'.'+crypto.randomUUID()+'.tmp';
  const handle=await fs.open(temp,'wx',0o600);
  try {await handle.writeFile(json(value));await handle.sync();}
  finally {await handle.close();}
  try {
    await fs.rename(temp,file);
    const directory=await fs.open(path.dirname(file),'r');
    try{await directory.sync();}finally{await directory.close();}
  } finally {await fs.rm(temp,{force:true});}
}

export async function startChannelManager(ctx,{
  directory=path.join(os.homedir(),'.czs','native-gui'),pollIntervalMs=1000,
  legacyPath=path.join(directory,'bridge.json'),
}={}) {
  if(typeof directory!=='string'||!path.isAbsolute(directory)||
      !Number.isSafeInteger(pollIntervalMs)||pollIntervalMs<10||pollIntervalMs>60000)
    throw new Error('invalid-manager-options');
  await fs.mkdir(directory,{recursive:true,mode:0o700});
  await privateDirectory(directory);
  const running=new Map();let revision=null;let lastStatus;let closing=false;let pending;
  let timer;let closePromise;

  async function start(id,file,errors) {
    try {
      const loaded=await readConfig(file);const config=loaded.value;
      const old=running.get(id);
      if(old){
        if(old.file!==file||old.digest!==loaded.digest)errors[id]='running-config-changed';
        return;
      }
      validateControlConfig(config);
      if(id==='legacy'?config.channelId!==undefined:config.channelId!==id)
        throw new Error('channel-id-mismatch');
      const paths=await Promise.all([config.socket,config.stateDir,config.receiptDir]
        .filter(value=>value!==undefined).map(value=>canonical(path.resolve(value))));
      for(const existing of running.values()){
        if(existing.config.sessionId===config.sessionId)throw new Error('session-conflict');
        if(paths.some(a=>existing.paths.some(b=>overlaps(a,b))))throw new Error('path-conflict');
      }
      if(id!=='legacy'){
        await privateDirectory(path.dirname(config.socket));
        await privateDirectory(config.stateDir);await privateDirectory(config.receiptDir);
      }
      const handle=await startControl(ctx,config);
      const ready={ready:true,channel_id:id,session_id:config.sessionId,socket:config.socket,
        state_dir:config.stateDir,...(config.channelId===undefined?{}:{
          owner_thread_id:config.ownerThreadId,receipt_dir:config.receiptDir}),
        started_at:new Date().toISOString()};
      running.set(id,{file,digest:loaded.digest,config,paths,handle,ready});
      if(id==='legacy')await writeRecord(path.join(directory,'ready.json'),{
        pid:process.pid,socket:config.socket,session_id:config.sessionId,started_at:ready.started_at});
    } catch(error) {
      // Config/parser/native error text may contain tokens; status uses fixed codes only.
      const allowed=new Set(['invalid-control-config','channel-id-mismatch','session-conflict',
        'path-conflict','unsafe-config','unsafe-directory','invalid-config-path','config-too-large',
        'config-changed-during-read','control-already-running','existing-control-endpoint']);
      errors[id]=allowed.has(error.message)?error.message:'channel-start-failed';
    }
  }

  async function refresh() {
    const errors=Object.create(null);
    let legacyExists=true;
    try {await fs.lstat(legacyPath);}catch(error){
      if(error.code==='ENOENT')legacyExists=false;else errors.legacy='legacy-config-unreadable';
    }
    if(legacyExists)await start('legacy',legacyPath,errors);
    else if(running.has('legacy'))errors.legacy='running-config-removed';
    let registry;
    try {
      registry=(await readConfig(path.join(directory,'channels.json'))).value;
      if(!registry||registry.schema_version!=='zsc.native-channels/v1'||
        !Number.isSafeInteger(registry.revision)||registry.revision<0||
        !registry.channels||Array.isArray(registry.channels)||typeof registry.channels!=='object')
        throw new Error('invalid-registry');
      revision=registry.revision;
    } catch(error) {
      if(error.code!=='ENOENT')errors._registry='registry-unreadable-or-invalid';
      else if([...running.keys()].some(id=>id!=='legacy'))errors._registry='registry-removed';
    }
    if(registry && !errors._registry){
      for(const [id,file] of Object.entries(registry.channels).sort(([a],[b])=>a.localeCompare(b))){
        if(!validId(id)){errors[id]='invalid-channel-id';continue;}
        await start(id,file,errors);
      }
      for(const id of running.keys()){
        if(id!=='legacy'&&!Object.hasOwn(registry.channels,id))errors[id]='running-config-removed';
      }
    }
    const channels=Object.fromEntries([...running].map(([id,item])=>[id,item.ready]));
    const status={schema_version:'zsc.native-channels-ready/v1',pid:process.pid,revision,channels,errors};
    const serialized=json(status);
    if(serialized!==lastStatus){await writeRecord(path.join(directory,'channels-ready.json'),status);lastStatus=serialized;}
    return status;
  }

  function reconcile() {
    if(closing)return Promise.resolve();
    if(!pending)pending=refresh().finally(()=>{pending=undefined;});
    return pending;
  }
  try {await reconcile();}
  catch(error){await Promise.allSettled([...running.values()].map(item=>item.handle.close()));throw error;}
  timer=setInterval(()=>{reconcile().catch(()=>{});},pollIntervalMs);timer.unref?.();
  return {reconcile,close(){
    if(closePromise)return closePromise;
    closing=true;clearInterval(timer);
    closePromise=(async()=>{
      try{await pending;}finally{
        const results=await Promise.allSettled([...running.values()].map(item=>item.handle.close()));
        const failed=results.find(result=>result.status==='rejected');
        if(failed)throw failed.reason;
      }
    })();
    return closePromise;
  }};
}
