// FF Lite — servidor multiplayer por salas (Node + ws). Start: node server.js
const http=require('http'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,MODES=['br','cs'],GRACE=25000,LOBBY=['nick','dev','kl','xp','sk','pts','st'];
const lw=s=>String(s||'').toLowerCase().slice(0,24),now=()=>Date.now();
const users=new Map(),rooms=new Map(),invs=new Map(),seen=new Map();let queue=[],seq=0;
const tx=(w,o)=>{if(w&&w.readyState===1)w.send(JSON.stringify(o))};
const to=(k,o)=>{const u=users.get(k);if(u&&!u.off)tx(u.ws,o)};
const other=(r,k)=>r.m.find(x=>x!==k);
const lobby=p=>{const o={};for(const f of LOBBY)o[f]=p[f];return o};
const mem=r=>r.m.map(x=>({nick:users.get(x).nick,sk:users.get(x).p.sk}));
const view=(r,k)=>({id:r.id,state:r.state,mm:r.mm,isHost:r.m[0]===k,seed:r.seed,mode:r.mode,members:mem(r)});
const board=()=>[...seen.values()].sort((a,b)=>b.pts-a.pts).slice(0,50);
function mkRoom(a,b,mm){const r={id:'R'+(++seq),m:[a,b],state:'waiting',mode:'br',seed:0,mm,ready:new Set()};rooms.set(r.id,r);
 for(const k of r.m)users.get(k).room=r.id;
 for(const k of r.m){to(k,{t:'room',room:view(r,k)});const o=users.get(other(r,k));if(o.p.nick)to(k,{t:'u',id:o.id,p:o.p})}return r}
function begin(r){if(r.state!=='starting')return;clearTimeout(r.tm);r.state='playing';for(const k of r.m)to(k,{t:'begin'})}
function start(r,md){r.state='starting';r.ready.clear();r.seed=1+(Math.random()*1e9|0);r.mode=md;
 for(const k of r.m)to(k,{t:'start',seed:r.seed,m:md,mm:r.mm,cf:r.cf,members:mem(r)});r.tm=setTimeout(()=>begin(r),12000)}
function leave(k,why){const u=users.get(k),r=u&&rooms.get(u.room);if(!r)return;u.room=null;r.m=r.m.filter(x=>x!==k);r.ready.delete(k);
 for(const o of r.m)to(o,{t:'left',nick:u.nick,why});
 if(!r.m.length){clearTimeout(r.tm);rooms.delete(r.id)}else if(r.state==='starting'&&r.ready.size>=r.m.length)begin(r)}
function pair(){queue=queue.filter(k=>{const x=users.get(k);return x&&!x.off&&!x.room});
 while(queue.length>=2){const a=queue.shift(),b=queue.shift();start(mkRoom(a,b,true),'br')}}
/* validação: velocidade máx., acertos só no parceiro de sala, alcance e dano limitados */
function clean(u,p){const r=rooms.get(u.room),o=r&&users.get(other(r,u.k)),t=now();
 if(p.x==null)u.lp=null;
 else{if(u.lp){const dt=Math.min(2,(t-u.lt)/1000),mx=90*dt+6,dx=p.x-u.lp.x,dz=p.z-u.lp.z,d=Math.hypot(dx,dz);if(d>mx){p.x=u.lp.x+dx/d*mx;p.z=u.lp.z+dz/d*mx}}u.lp={x:p.x,z:p.z};u.lt=t}
 const ok=r&&r.state==='playing'&&o&&o.p.x!=null&&p.x!=null;let nw=0;
 p.hq=ok&&Array.isArray(p.hq)?p.hq.filter(h=>{if(!h||h.to!==lw(o.nick)||!(h.dm>0&&h.dm<=150)||Math.hypot(p.x-o.p.x,p.z-o.p.z)>400)return false;
  if(!u.hs.has(h.id)){u.hs.set(h.id,t);nw++}return true}).slice(-8):[];
 if(nw>30)p.hq=[];
 if(u.room){delete p.q;delete p.mm}
 if(u.hs.size>300)for(const[i,v]of u.hs)if(t-v>6000)u.hs.delete(i)}
const wss=new WebSocketServer({server:http.createServer((q,r)=>r.end('FF Lite server ok')).listen(PORT)});
wss.on('connection',ws=>{let u=null;const err=msg=>tx(ws,{t:'err',msg});
 ws.on('message',buf=>{let m;try{m=JSON.parse(buf)}catch(e){return}
  if(m.t==='p'&&m.p&&m.p.nick){const k=lw(m.p.nick);
   if(!u){const old=users.get(k);
    if(old){u=old;const w=u.ws,was=u.off;u.ws=ws;u.off=0;clearTimeout(u.gt);if(w!==ws)try{w.close()}catch(e){}
     if(was&&u.room){const r=rooms.get(u.room);for(const o of r.m)if(o!==k)to(o,{t:'back',nick:u.nick})}}
    else{u={k,nick:m.p.nick,ws,id:m.id,p:{},room:null,off:0,lp:null,lt:0,hs:new Map(),rt:0};users.set(k,u)}
    u.id=m.id;
    tx(ws,{t:'all',list:[...users.values()].filter(x=>x!==u&&!x.off&&x.p.nick).map(x=>({id:x.id,p:lobby(x.p)}))});tx(ws,{t:'board',list:board()});
    const r=rooms.get(u.room);if(r){tx(ws,{t:'room',room:view(r,k)});const o=users.get(other(r,k));if(o&&o.p.nick)tx(ws,{t:'u',id:o.id,p:o.p})}}
   if(k!==u.k)return;const t=now();if(t-u.rt<60)return;u.rt=t;
   u.p=m.p;clean(u,u.p);seen.set(k,{nick:u.nick,pts:+u.p.pts||0,kills:+u.p.kl||0,xp:+u.p.xp||0,dev:u.p.dev});
   const r=rooms.get(u.room);
   for(const x of users.values()){if(x===u||x.off)continue;tx(x.ws,{t:'u',id:u.id,p:r&&r.m.includes(x.k)?u.p:lobby(u.p)})}return}
  if(!u)return;
  if(m.t==='inv'){const tk=lw(m.to),v=users.get(tk);
   if(u.room)return err('Você já está em uma partida. Termine antes de convidar.');
   if(!v||v.off||v===u)return err('Jogador indisponível');
   if(v.room||(v.p.st&&v.p.st!=='menu'))return err(v.nick+' já está em uma partida');
   for(const[i,x]of invs)if(x.f===u.k)invs.delete(i);
   const iid='I'+(++seq);invs.set(iid,{f:u.k,to:tk});tx(v.ws,{t:'inv',iid,from:u.nick,sk:u.p.sk});tx(ws,{t:'invst',st:'sent',to:v.nick});
   setTimeout(()=>{const x=invs.get(iid);if(x){invs.delete(iid);to(x.f,{t:'invst',st:'expired',to:v.nick})}},30000)}
  else if(m.t==='ans'){const i=invs.get(m.iid);if(!i||i.to!==u.k)return err('Convite expirado');invs.delete(m.iid);const f=users.get(i.f);
   if(!m.ok){to(i.f,{t:'invst',st:'declined',to:u.nick});return}
   if(!f||f.off||f.room||u.room)return err('Um dos jogadores já está em uma partida');
   to(i.f,{t:'invst',st:'accepted',to:u.nick});mkRoom(i.f,u.k,false)}
  else if(m.t==='go'){const r=rooms.get(u.room);
   if(!r||r.mm||r.m[0]!==u.k)return err('Só quem convidou pode iniciar a dupla');
   if(r.state!=='waiting')return err('A partida já está em andamento');
   if(r.m.length<2)return err('Seu parceiro saiu da sala');
   const md=String(m.m||'br');if(!MODES.includes(md))return err('Dupla online só funciona em BR e CS (não no X1)');
   if(users.get(r.m[1]).off)return err('Seu parceiro está reconectando');
   r.cf={mode:(m.cf&&m.cf.mode)|0};start(r,md)}
  else if(m.t==='ready'){const r=rooms.get(u.room);if(r&&r.state==='starting'){r.ready.add(u.k);if(r.ready.size>=r.m.length)begin(r)}}
  else if(m.t==='end')leave(u.k,'end');
  else if(m.t==='q'){if(u.room)return err('Você já está em uma partida. Termine antes de iniciar outra.');
   if(!MODES.includes(String(m.m||'br')))return err('Online só em BR e CS');
   queue=queue.filter(x=>x!==u.k).concat(u.k);pair();clearTimeout(u.qt);
   u.qt=setTimeout(()=>{if(queue.includes(u.k)){queue=queue.filter(x=>x!==u.k);tx(u.ws,{t:'start',seed:1+(Math.random()*1e9|0),mm:true,solo:1,members:[{nick:u.nick,sk:u.p.sk}]})}},25000)}
  else if(m.t==='qc'){queue=queue.filter(x=>x!==u.k);clearTimeout(u.qt)}});
 ws.on('close',()=>{if(!u||u.ws!==ws)return;queue=queue.filter(x=>x!==u.k);for(const x of users.values())if(x!==u)tx(x.ws,{t:'l',id:u.id});
  const r=rooms.get(u.room);if(!r){users.delete(u.k);return}
  u.off=now();for(const o of r.m)if(o!==u.k)to(o,{t:'off',nick:u.nick});
  u.gt=setTimeout(()=>{if(u.off){leave(u.k,'timeout');users.delete(u.k)}},GRACE)})});
setInterval(()=>{const b=JSON.stringify({t:'board',list:board()});for(const u of users.values())if(!u.off&&u.ws.readyState===1)u.ws.send(b)},20000);
