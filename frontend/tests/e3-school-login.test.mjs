import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {createSchoolLoginController, SCHOOL_LOGIN_ENTRY} from '../assignments/static/e3-navigation-extension/core.js';

const requestId='12345678-1234-1234-1234-123456789012';
const source={frameId:0,tab:{id:1,windowId:3},documentId:'source-doc',url:'https://www.e3hwtool.space/login'};
const e3={frameId:0,tab:{id:10},documentId:'school-doc',url:'https://e3p.nycu.edu.tw/my/'};
function fixture(){
 const records={}, changes=[], releases=[]; let time=1000;
 const storage={async get(k){return {[k]:records[k]};},async set(v){Object.assign(records,v);},async remove(k){delete records[k];}};
 const tabs={async create(v){changes.push(v);return{id:10};},async update(id,v){changes.push({id,...v});}};
 const controller=createSchoolLoginController({storage,tabs,now:()=>time,transfer:async(...v)=>{releases.push(v);return true;}});
 return {controller,records,changes,releases,advance:()=>{time+=11*60*1000;}};
}
const begin=f=>f.controller.handle({type:'OPEN_E3_SCHOOL_LOGIN',requestId},source);
const finish=f=>f.controller.handle({type:'E3_SCHOOL_LOGIN_STATE',status:'authenticated'},e3);

test('school login returns to the exact initiating document once, without storing credentials',async()=>{
 const f=fixture();assert.equal((await begin(f)).ok,true);
 assert.equal(f.changes.at(-1).url,SCHOOL_LOGIN_ENTRY);
 assert.equal((await f.controller.handle({type:'E3_SCHOOL_LOGIN_PENDING'},e3)).pending,true);
 assert.equal((await finish(f)).ok,true);
 assert.equal(f.releases.length,1);
 assert.deepEqual(f.releases[0],[{tabId:1,documentId:'source-doc',requestId,origin:'https://www.e3hwtool.space'},10]);
 assert.deepEqual(f.records,{});
 await finish(f);assert.equal(f.releases.length,1);
 assert.deepEqual(f.changes.at(-1),{id:1,active:true,url:'https://www.e3hwtool.space/'});
});
test('unrelated pages, frames, source documents and expired flows cannot release credentials',async()=>{
 const f=fixture();
 for(const sender of [{...source,frameId:1},{...source,url:'https://evil.test/login'},{...source,url:'https://www.e3hwtool.space/'},{...source,documentId:undefined}]){
  assert.equal((await f.controller.handle({type:'OPEN_E3_SCHOOL_LOGIN',requestId},sender)).ok,false);
 }
 await begin(f);
 assert.equal((await f.controller.handle({type:'E3_SCHOOL_LOGIN_STATE',status:'authenticated'},{...e3,url:'https://evil.test/my/'})).ok,false);
 f.advance();await finish(f);assert.equal(f.releases.length,0);assert.deepEqual(f.records,{});
});
test('unknown login, MFA and duplicate reports never transfer early or more than once',async()=>{
 const f=fixture();await begin(f);
 for(const status of ['unknown','login-required']) await f.controller.handle({type:'E3_SCHOOL_LOGIN_STATE',status},e3);
 assert.equal(f.releases.length,0);
 await Promise.all([finish(f),finish(f)]);assert.equal(f.releases.length,1);
});
test('cookie transfer is confined to the original profile and document',async()=>{
 const code=readFileSync(new URL('../assignments/static/e3-navigation-extension/background.js',import.meta.url),'utf8').replace(/^import [^\r\n]*\r?\n/,'');
 const sent=[];
 const chrome={storage:{session:{}},runtime:{onMessage:{addListener(){}}},tabs:{onRemoved:{addListener(){}},async sendMessage(...v){sent.push(v);return{ok:true};}},
  cookies:{async getAllCookieStores(){return[{id:'normal',tabIds:[1,10]}];},async get(v){assert.equal(v.storeId,'normal');return{value:'synthetic-session-1234567890'};}}};
 const scope=vm.createContext({chrome,E3_ORIGIN:'https://e3p.nycu.edu.tw',createNavigationController:()=>({}),createSchoolLoginController:()=>({})});vm.runInContext(code,scope);
 assert.equal(await scope.transferSchoolSession({tabId:1,documentId:'source-doc',requestId},10),true);
 assert.equal(sent[0][2].documentId,'source-doc');
 chrome.cookies.getAllCookieStores=async()=>[{id:'private',tabIds:[10]}];
 assert.equal(await scope.transferSchoolSession({tabId:1,documentId:'source-doc',requestId},10),false);assert.equal(sent.length,1);
});
test('the login bridge requires a trusted click, consumes its nonce and keeps CSRF and same-origin validation',async()=>{
 const code=readFileSync(new URL('../assignments/static/e3-navigation-extension/tracker.js',import.meta.url),'utf8');
 let click,listener;const requests=[];
 const button={dataset:{},addEventListener:(type,fn)=>{click=fn;}};
 const status={textContent:''};
 const document={readyState:'complete',getElementById:id=>id==='schoolLoginButton'?button:status,addEventListener(){},querySelector:()=>({content:'csrf-test'}),body:{dataset:{}}};
 const location={pathname:'/login',origin:'https://www.e3hwtool.space'};
 const scope=vm.createContext({document,location,URL,URLSearchParams,AbortSignal,Date,setTimeout(){},crypto:{randomUUID:()=>requestId},
  window:{location:{assign:url=>requests.push({navigate:url})}},chrome:{runtime:{id:'own',onMessage:{addListener:fn=>{listener=fn;}},async sendMessage(){return{ok:true};}}},
  fetch:async(url,options)=>{requests.push({url,options});return{ok:true,url:location.origin+'/'};}});
 vm.runInContext(code,scope);
 const message={type:'E3_SCHOOL_LOGIN_SESSION',requestId,session:'synthetic-session-1234567890'};
 const deliver=(sender={id:'own'})=>new Promise(resolve=>{if(listener(message,sender,resolve)!==true)resolve({ok:false});});
 click({isTrusted:false});assert.equal((await deliver()).ok,false);
 click({isTrusted:true});assert.equal((await deliver({id:'other'})).ok,false);
 assert.equal((await deliver()).ok,true);
 assert.equal(requests[0].options.headers['X-CSRFToken'],'csrf-test');assert.equal(requests[0].options.mode,'same-origin');
 const body=new URLSearchParams(requests[0].options.body);assert.equal(body.get('admin_session'),'1');assert.equal(body.get('moodle_session'),message.session);
 assert.equal((await deliver()).ok,false);
});
