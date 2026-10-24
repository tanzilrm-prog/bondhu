const express=require('express'),http=require('http'),{Server}=require('socket.io'),crypto=require('crypto'),fs=require('fs'),path=require('path'),dns=require('dns'),net=require('net'),https=require('https'),zlib=require('zlib');
const DBURL=process.env.DATABASE_URL;let pool=null,PRE=null;
(async()=>{
 if(DBURL){const {Pool}=require('pg');pool=new Pool({connectionString:DBURL,ssl:/localhost|127\.0\.0\.1/.test(DBURL)?false:{rejectUnauthorized:false},max:4});
  await pool.query('create table if not exists kv(k text primary key,v text not null)');
  await pool.query('create table if not exists files(name text primary key,mime text,data bytea not null,t bigint)');
  const r=await pool.query("select v from kv where k='data'");if(r.rows[0])PRE=r.rows[0].v;
  console.log('Database ON'+(PRE?' (data loaded)':' (empty, starting fresh/file)'))}
 main()})().catch(e=>{console.error('Start failed:',e.message);process.exit(1)});
function main(){
const app=express(),srv=http.createServer(app),io=new Server(srv);
const F=process.env.DATA_FILE||path.join(__dirname,'data.json');
const UP=process.env.UPLOAD_DIR||path.join(__dirname,'uploads');fs.mkdirSync(UP,{recursive:true});
let D={users:[],posts:[],fr:[],rq:[],msgs:[],tok:{},bl:[],mt:[],nt:[],rp:[],cfg:{},pages:[],pf:[],ps:[],stories:[]};
try{D=Object.assign(D,JSON.parse(PRE!=null?PRE:fs.readFileSync(F,'utf8')))}catch(e){}
D.posts.forEach(p=>{if(!p.rx){p.rx={};(p.likes||[]).forEach(u=>p.rx[u]='like');delete p.likes}});
let st,chain=Promise.resolve();
const wr=()=>chain=chain.then(()=>pool?pool.query("insert into kv(k,v) values('data',$1) on conflict(k) do update set v=excluded.v",[JSON.stringify(D)]).catch(e=>console.log('DB save fail:',e.message)):new Promise(res=>fs.writeFile(F,JSON.stringify(D),()=>res())));
const save=()=>{clearTimeout(st);st=setTimeout(wr,pool?1200:300)};
process.on('SIGTERM',()=>{clearTimeout(st);wr().then(()=>process.exit(0))});
const MIME={mp4:'video/mp4',webm:'video/webm',mov:'video/quicktime',jpg:'image/jpeg'};
function putFile(n,buf,cb){fs.writeFile(path.join(UP,n),buf,e=>{if(e)return cb(e);if(!pool)return cb();
 pool.query('insert into files(name,mime,data,t) values($1,$2,$3,$4) on conflict(name) do nothing',[n,MIME[n.split('.').pop()],buf,Date.now()]).then(()=>cb(),er=>{console.log('DB file fail:',er.message);cb(er)})})}
const ADM=(process.env.ADMIN_NAME||'').trim().toLowerCase(),MAX=()=>D.cfg.max||+process.env.MAX_USERS||10;
let lu=0;let wp=null;try{wp=require('web-push')}catch(e){}
const VP={};
function initPush(){if(!wp)return;let pu=process.env.VAPID_PUBLIC,pr=process.env.VAPID_PRIVATE;
 if(!(pu&&pr)){D.cfg.vapid=D.cfg.vapid||wp.generateVAPIDKeys();pu=D.cfg.vapid.publicKey;pr=D.cfg.vapid.privateKey;save()}
 try{wp.setVapidDetails(process.env.VAPID_SUBJECT||'mailto:bondhu@users.noreply.github.com',pu,pr);VP.pub=pu}catch(e){console.log('Push off:',e.message);wp=null}}
initPush();
const SD=864e5;
const SEED=[['BBC News বাংলা','https://feeds.bbci.co.uk/bengali/rss.xml'],['Prothom Alo','https://www.prothomalo.com/feed'],['The Daily Star','https://www.thedailystar.net/frontpage/rss.xml'],['Dhaka Tribune','https://www.dhakatribune.com/feed']];
const uid=()=>{let i=Date.now()*1000+(Math.random()*1000|0);if(i<=lu)i=lu+1;return lu=i};
if(!D.cfg.seeded){SEED.forEach(([name,rss])=>D.pages.push({id:uid(),name,rss,ver:1}));D.cfg.seeded=1;save()}
const hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex');
const isFol=(u,g)=>D.pf.some(x=>x.u==u&&x.pg==g);
const isF=(a,b)=>D.fr.some(f=>f.includes(a)&&f.includes(b));
const blocked=(a,b)=>D.bl.some(x=>(x.by==a&&x.t==b)||(x.by==b&&x.t==a));
const thr=(a,b)=>D.mt.find(t=>(t.a==a&&t.b==b)||(t.a==b&&t.b==a));
const canTalk=(a,b)=>!blocked(a,b)&&(isF(a,b)||(thr(a,b)||{}).st=='ok');
const canSee=(p,m)=>p.pg?isFol(m,p.pg):p.uid==m||!p.vis||p.vis=='pub'||(p.vis=='fr'&&isF(p.uid,m))||(p.vis=='cus'&&(p.allow||[]).includes(m))||(p.vis!='me'&&(p.tags||[]).includes(m));
const ub=id=>(D.users.find(u=>u.id==id)||{}).ban;
const MT={'video/mp4':'mp4','video/webm':'webm','video/quicktime':'mov','image/jpeg':'jpg'},MEDIA=/^\/uploads\/[a-f0-9]{24}\.(mp4|webm|mov|jpg)$/,RXK=['like','love','haha','wow','sad','angry','dislike','fire'];
const on=new Map(),ping=(...ids)=>ids.forEach(i=>io.to('u'+i).emit('sync'));
function pushTo(to,pl){if(!wp)return;const b=JSON.stringify(pl);D.ps.filter(x=>x.u==to).forEach(x=>wp.sendNotification(x.sub,b,{TTL:3600,urgency:'high'}).catch(e=>{if(e.statusCode==404||e.statusCode==410){D.ps=D.ps.filter(y=>y!==x);save()}}))}
const nameOf=id=>(D.users.find(u=>u.id==id)||{name:'Keu ekjon'}).name;
function notify(to,ty,f,tx,pid){if(to==f||!D.users.some(u=>u.id==to))return;if(D.nt.some(n=>n.to==to&&n.ty==ty&&n.f==f&&n.pid==pid&&!n.r))return;
 D.nt.push({id:uid(),to,ty,f,tx,pid,t:Date.now(),r:0});if(D.nt.length>1000)D.nt.splice(0,D.nt.length-1000);ping(to);pushTo(to,{title:'Bondhu',body:nameOf(f)+' '+tx,tag:ty+':'+f})}
const unf=u=>{if(u){const n=path.basename(u);fs.unlink(path.join(UP,n),()=>{});if(pool)pool.query('delete from files where name=$1',[n]).catch(()=>{})}};
const unl=p=>unf(p.vid),unm=a=>a.forEach(x=>x.m&&unf(x.m.u));
function rmUser(id){D.stories.filter(s=>s.u==id).forEach(s=>unf(s.url));D.stories=D.stories.filter(s=>s.u!=id);D.posts.filter(p=>p.uid==id).forEach(unl);unf((D.users.find(u=>u.id==id)||{}).pic);unm(D.msgs.filter(x=>x.a==id||x.b==id));D.posts=D.posts.filter(p=>p.uid!=id);
 D.posts.forEach(p=>{delete p.rx[id];p.c=p.c.filter(c=>c.uid!=id);p.tags=(p.tags||[]).filter(i=>i!=id);p.allow=(p.allow||[]).filter(i=>i!=id)});
 D.users=D.users.filter(u=>u.id!=id);D.fr=D.fr.filter(f=>!f.includes(id));D.rq=D.rq.filter(x=>x.from!=id&&x.to!=id);
 D.msgs=D.msgs.filter(x=>x.a!=id&&x.b!=id);D.mt=D.mt.filter(t=>t.a!=id&&t.b!=id);D.bl=D.bl.filter(x=>x.by!=id&&x.t!=id);
 D.nt=D.nt.filter(n=>n.to!=id&&n.f!=id);D.rp=D.rp.filter(r=>r.by!=id&&!(r.type=='user'&&r.tid==id));
 D.pf=D.pf.filter(x=>x.u!=id);D.ps=D.ps.filter(x=>x.u!=id);
 for(const k in D.tok)if(D.tok[k]==id)delete D.tok[k];OTP.delete(id);
 io.in('u'+id).disconnectSockets(true);save();io.emit('sync')}
function rmPost(p){unl(p);D.posts=D.posts.filter(x=>x!==p);D.rp=D.rp.filter(r=>!(r.type=='post'&&r.tid==p.id));save();io.emit('sync')}

app.use(express.json({limit:'4mb'}));
app.use(express.static(path.join(__dirname,'public')));
app.use('/uploads',express.static(UP,{maxAge:'30d'}));
app.get('/uploads/:n',(q,r)=>{const n=path.basename(q.params.n);if(!pool||!/^[a-f0-9]{24}\.(mp4|webm|mov|jpg)$/.test(n))return r.sendStatus(404);
 pool.query('select data from files where name=$1',[n]).then(x=>{if(!x.rows[0])return r.sendStatus(404);const f=path.join(UP,n);fs.writeFile(f,x.rows[0].data,e=>e?r.sendStatus(500):r.sendFile(f))}).catch(()=>r.sendStatus(500))});
const auth=(q,r,n)=>{const id=D.tok[(q.headers.authorization||'').slice(7)],u=D.users.find(u=>u.id==id);
 if(!u||u.ban)return r.status(401).json({e:'Login koro'});q.me=id;q.u=u;n()};
const adm=(q,r,n)=>auth(q,r,()=>q.u.admin?n():r.status(403).json({e:'Admin na'}));
const pub=u=>({id:u.id,name:u.name,pic:u.pic||null,ver:!!u.vf});
function login(u,r){const tk=crypto.randomBytes(24).toString('hex');D.tok[tk]=u.id;save();r.json({token:tk})}

app.post('/api/signup',(q,r)=>{const n=String(q.body.name||'').trim().slice(0,30),p=String(q.body.pw||'');
 if(!n||p.length<4)return r.status(400).json({e:'Naam dao, password minimum 4 character'});
 const isA=!!ADM&&n.toLowerCase()==ADM;
 if(isA&&p!==process.env.ADMIN_PASSWORD)return r.status(400).json({e:'Ei naam reserved'});
 if(D.users.some(u=>u.name.toLowerCase()==n.toLowerCase()))return r.status(400).json({e:'Ei naam ache'});
 if(!isA&&D.users.length>=MAX())return r.status(400).json({e:'User limit ('+MAX()+' jon) pura hoye geche'});
 const salt=crypto.randomBytes(8).toString('hex'),u={id:uid(),name:n,salt,h:hash(p,salt),admin:isA};D.users.push(u);io.emit('sync');login(u,r)});
app.post('/api/login',(q,r)=>{const u=D.users.find(u=>u.name.toLowerCase()==String(q.body.name||'').trim().toLowerCase());
 if(!u||u.h!=hash(String(q.body.pw||''),u.salt))return r.status(400).json({e:'Vul naam ba password'});
 if(u.ban)return r.status(403).json({e:'Tomar account ban kora hoyeche'});login(u,r)});

app.post('/api/logout',(q,r)=>{delete D.tok[(q.headers.authorization||'').slice(7)];save();r.json({ok:1})});
app.get('/api/state',auth,(q,r)=>{const m=q.me,A=q.u.admin;
 r.json({me:m,admin:!!A,users:D.users.filter(u=>(A||!u.ban)&&!D.bl.some(x=>x.by==u.id&&x.t==m)).map(pub),
 posts:D.posts.filter(p=>!p.del&&!p.pg&&!blocked(m,p.uid)&&canSee(p,m)&&(A||!ub(p.uid))).slice(-100).concat(D.posts.filter(p=>!p.del&&p.pg&&isFol(m,p.pg)).slice(-60)).sort((a,b)=>a.t-b.t),
 pages:D.pages.map(g=>({id:g.id,name:g.name,ver:!!g.ver,fol:D.pf.filter(x=>x.pg==g.id).length})),pf:D.pf.filter(x=>x.u==m).map(x=>x.pg),
 mut:Object.fromEntries(D.users.filter(u=>u.id!=m).map(u=>[u.id,D.fr.filter(f=>f.includes(m)).map(f=>f[0]==m?f[1]:f[0]).filter(x=>x!=u.id&&isF(x,u.id)).length])),
 trash:D.posts.filter(p=>p.uid==m&&p.del).sort((a,b)=>a.del-b.del).slice(-100),
 fr:D.fr.filter(f=>f.includes(m)),rq:D.rq.filter(x=>x.from==m||x.to==m),
 msgs:D.msgs.filter(x=>(x.a==m||x.b==m)&&!blocked(x.a,x.b)&&!(x.hd||[]).includes(m)).slice(-500),
 stories:D.stories.filter(s=>Date.now()-s.t<SD&&!blocked(m,s.u)&&(s.u==m||(isF(m,s.u)&&!ub(s.u)))).map(s=>s.u==m?s:{id:s.id,u:s.u,url:s.url,k:s.k,cap:s.cap,t:s.t}),
 bl:D.bl.filter(x=>x.by==m).map(x=>x.t),mt:D.mt.filter(t=>t.a==m||t.b==m),
 nt:D.nt.filter(n=>n.to==m&&!blocked(m,n.f)).slice(-40).reverse(),online:[...on.keys()],my:{em:q.u.em||'',ph:q.u.ph||''},vc:{em:mailOn()||devOtp(),ph:smsOn()||devOtp()}})});

app.post('/api/post',auth,(q,r)=>{const text=String(q.body.text||'').slice(0,2000),
 img=typeof q.body.img=='string'&&q.body.img.startsWith('data:image/')?q.body.img:null,
 vid=typeof q.body.vid=='string'&&/^\/uploads\/[a-f0-9]{24}\.(mp4|webm|mov)$/.test(q.body.vid)?q.body.vid:null,
 tags=(Array.isArray(q.body.tags)?q.body.tags:[]).map(Number).filter(i=>isF(q.me,i)&&!blocked(q.me,i)).slice(0,10);
 if(!text&&!img&&!vid)return r.status(400).json({e:'Khali post'});
 const vis=['pub','fr','cus','me'].includes(q.body.vis)?q.body.vis:'pub',allow=vis=='cus'?(Array.isArray(q.body.allow)?q.body.allow:[]).map(Number).filter(i=>isF(q.me,i)&&!blocked(q.me,i)).slice(0,100):[];
 const p={id:uid(),uid:q.me,text,img,vid,tags,vis,allow,t:Date.now(),rx:{},c:[]};D.posts.push(p);
 if(vis!='me')tags.filter(i=>canSee(p,i)).forEach(i=>notify(i,'tag',q.me,'tomake post e tag korechhe',p.id));
 save();io.emit('sync');r.json({ok:1})});
app.post('/api/post/:id/del',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id);
 if(p&&!p.del){if(p.uid==q.me){p.del=Date.now();save();io.emit('sync')}else if(q.u.admin)rmPost(p)}r.json({ok:1})});
app.post('/api/post/:id/restore',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id);
 if(p&&p.uid==q.me&&p.del){delete p.del;save();io.emit('sync')}r.json({ok:1})});
app.post('/api/post/:id/purge',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id);
 if(p&&p.uid==q.me&&p.del)rmPost(p);r.json({ok:1})});
app.post('/api/trash/empty',auth,(q,r)=>{D.posts.filter(p=>p.uid==q.me&&p.del).forEach(rmPost);r.json({ok:1})});
app.post('/api/upload',auth,express.raw({type:Object.keys(MT),limit:'80mb'}),(q,r)=>{
 const ext=MT[q.headers['content-type']];if(!ext||!Buffer.isBuffer(q.body)||!q.body.length)return r.status(400).json({e:'Shudhu photo ba mp4/webm/mov video dao (80MB porjonto)'});
 const n=crypto.randomBytes(12).toString('hex')+'.'+ext;
 putFile(n,q.body,e=>e?r.status(500).json({e:'Save hoyni'}):r.json({url:'/uploads/'+n}))});
app.post('/api/react/:id/:k',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id),k=q.params.k;
 if(p&&!p.del&&RXK.includes(k)&&canSee(p,q.me)&&!blocked(q.me,p.uid)){if(p.rx[q.me]==k)delete p.rx[q.me];else{p.rx[q.me]=k;notify(p.uid,'rx',q.me,'tomar post e react korechhe',p.id)}save();io.emit('sync')}r.json({ok:1})});
app.post('/api/comment/:id',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id),text=String(q.body.text||'').slice(0,500);
 if(p&&!p.del&&text&&canSee(p,q.me)&&!blocked(q.me,p.uid)){p.c.push({uid:q.me,text});notify(p.uid,'cm',q.me,'tomar post e comment korechhe',p.id);save();io.emit('sync')}r.json({ok:1})});

app.post('/api/fr/:id/:a',auth,(q,r)=>{const m=q.me,o=+q.params.id,a=q.params.a;
 if(!D.users.some(u=>u.id==o)||o==m)return r.status(400).json({e:'Vul user'});
 const had=D.rq.some(x=>x.from==o&&x.to==m),mine=D.rq.some(x=>x.from==m&&x.to==o),fr=isF(m,o);
 D.rq=D.rq.filter(x=>!(x.from==m&&x.to==o)&&!(x.from==o&&x.to==m));
 if(a=='acc'&&had){D.fr.push([m,o]);notify(o,'fra',m,'tomar friend request accept korechhe')}
 else if(a=='rm')D.fr=D.fr.filter(f=>!(f.includes(m)&&f.includes(o)));
 else if(a=='add'&&!fr&&!had&&!mine&&!blocked(m,o)){D.rq.push({from:m,to:o});notify(o,'fr',m,'tomake friend request pathiyeche')}
 save();ping(m,o);r.json({ok:1})});
app.post('/api/msg/:id',auth,(q,r)=>{const m=q.me,o=+q.params.id,text=String(q.body.text||'').slice(0,2000),mu=String((q.body.m||{}).url||''),M=MEDIA.test(mu)?{u:mu,k:mu.endsWith('.jpg')?'img':'vid'}:null;
 if((!text&&!M)||o==m||!D.users.some(u=>u.id==o))return r.status(400).json({e:'Vul request'});
 if(blocked(m,o))return r.status(403).json({e:'Message pathano jabe na'});
 let nw=0;if(!canTalk(m,o)){const t=thr(m,o);
  if(!t){nw=1;D.mt.push({a:m,b:o,st:'pending'});notify(o,'mr',m,'tomake message request pathiyeche')}
  else if(t.st=='pending'&&t.b==m)t.st='ok';
  else if(D.msgs.some(x=>x.a==m&&x.b==o))return r.status(400).json({e:'O accept korar age aro message pathano jabe na'})}
 D.msgs.push(Object.assign({id:uid(),a:m,b:o,text,t:Date.now()},M?{m:M}:{}));
 if(!nw&&!/^(📞|🎥) /.test(text))pushTo(o,{title:nameOf(m),body:text||(M.k=='img'?'📷 Photo':'🎬 Video'),tag:'m'+m});
 save();ping(m,o);r.json({ok:1})});
app.post('/api/mr/:id/:a',auth,(q,r)=>{const m=q.me,o=+q.params.id,t=thr(m,o);
 if(t&&t.b==m&&t.st=='pending'){if(q.params.a=='acc')t.st='ok';else{D.mt=D.mt.filter(x=>x!==t);unm(D.msgs.filter(x=>(x.a==m&&x.b==o)||(x.a==o&&x.b==m)));D.msgs=D.msgs.filter(x=>!((x.a==m&&x.b==o)||(x.a==o&&x.b==m)))}}
 save();ping(m,o);r.json({ok:1})});
app.post('/api/block/:id/:v',auth,(q,r)=>{const m=q.me,o=+q.params.id;
 if(!D.users.some(u=>u.id==o)||o==m)return r.status(400).json({e:'Vul user'});
 D.bl=D.bl.filter(x=>!(x.by==m&&x.t==o));
 if(q.params.v=='1'){D.bl.push({by:m,t:o});D.fr=D.fr.filter(f=>!(f.includes(m)&&f.includes(o)));D.rq=D.rq.filter(x=>!(x.from==m&&x.to==o)&&!(x.from==o&&x.to==m))}
 save();ping(m,o);r.json({ok:1})});

app.post('/api/profile/pic',auth,express.raw({type:'image/jpeg',limit:'1mb'}),(q,r)=>{
 if(!Buffer.isBuffer(q.body)||!q.body.length)return r.status(400).json({e:'Shudhu jpeg photo dao'});
 const n=crypto.randomBytes(12).toString('hex')+'.jpg';
 putFile(n,q.body,e=>{if(e)return r.status(500).json({e:'Save hoyni'});unf(q.u.pic);q.u.pic='/uploads/'+n;save();io.emit('sync');r.json({ok:1})})});
app.post('/api/profile/pic/del',auth,(q,r)=>{unf(q.u.pic);delete q.u.pic;save();io.emit('sync');r.json({ok:1})});
app.post('/api/report',auth,(q,r)=>{const type=q.body.type=='user'?'user':'post',tid=+q.body.id,reason=String(q.body.reason||'').trim().slice(0,300);
 const ok=type=='post'?D.posts.some(p=>p.id==tid&&!p.del&&canSee(p,q.me)):D.users.some(u=>u.id==tid&&u.id!=q.me);
 if(!reason||!ok)return r.status(400).json({e:'Report hoyni'});
 if(!D.rp.some(x=>x.by==q.me&&x.type==type&&x.tid==tid)){D.rp.push({id:uid(),by:q.me,type,tid,reason,t:Date.now()});
  D.users.filter(u=>u.admin).forEach(a=>notify(a.id,'adm',q.me,'ekta '+type+' report korechhe'))}
 save();r.json({ok:1})});
app.post('/api/nt/read',auth,(q,r)=>{D.nt.forEach(n=>{if(n.to==q.me)n.r=1});save();ping(q.me);r.json({ok:1})});
app.post('/api/delete-account',auth,(q,r)=>{if(q.u.admin)return r.status(400).json({e:'Admin account eikhan theke delete hobe na'});
 if(q.u.h!=hash(String(q.body.pw||''),q.u.salt))return r.status(400).json({e:'Password vul'});rmUser(q.me);r.json({ok:1})});


/* ---- Tools: name, password, OTP verify ---- */
const lim={};const hit=(k,n,ms)=>{const t=Date.now(),a=(lim[k]||[]).filter(x=>t-x<ms);if(a.length>=n){lim[k]=a;return false}a.push(t);lim[k]=a;return true};
setInterval(()=>{const t=Date.now();for(const k in lim){lim[k]=lim[k].filter(x=>t-x<36e5);if(!lim[k].length)delete lim[k]}for(const [k,o] of OTP)if(t>o.exp)OTP.delete(k)},6e5).unref();
app.post('/api/profile/name',auth,(q,r)=>{const n=String(q.body.name||'').trim().slice(0,30);
 if(!n)return r.status(400).json({e:'Naam dao'});
 if(ADM&&n.toLowerCase()==ADM&&!q.u.admin)return r.status(400).json({e:'Ei naam reserved'});
 if(D.users.some(u=>u.id!=q.me&&u.name.toLowerCase()==n.toLowerCase()))return r.status(400).json({e:'Ei naam ache'});
 q.u.name=n;save();io.emit('sync');r.json({ok:1})});
app.post('/api/profile/password',auth,(q,r)=>{const o=String(q.body.old||''),p=String(q.body.pw||'');
 if(!hit('pw'+q.me,5,6e5))return r.status(429).json({e:'Onek bar chesta hoyeche, kichukkhon pore try koro'});
 if(q.u.h!=hash(o,q.u.salt))return r.status(400).json({e:'Aager password vul'});
 if(p.length<4)return r.status(400).json({e:'Notun password minimum 4 character'});
 if(p==o)return r.status(400).json({e:'Notun password aager tar moto na'});
 const salt=crypto.randomBytes(8).toString('hex'),cur=(q.headers.authorization||'').slice(7);q.u.salt=salt;q.u.h=hash(p,salt);
 for(const k in D.tok)if(D.tok[k]==q.me&&k!=cur)delete D.tok[k];
 io.in('u'+q.me).fetchSockets().then(l=>l.filter(x=>x.handshake.auth.token!=cur).forEach(x=>x.disconnect(true))).catch(()=>{});
 save();r.json({ok:1})});

const OTP=new Map();
const mailOn=()=>!!((process.env.BREVO_API_KEY||process.env.RESEND_API_KEY)&&process.env.MAIL_FROM),smsOn=()=>!!(process.env.TWILIO_SID&&process.env.TWILIO_TOKEN&&process.env.TWILIO_FROM),devOtp=()=>process.env.OTP_DEV=='1';
const emailOk=e=>e.length<=120&&/^[^\s@]{1,64}@[^\s@]+\.[^\s@]{2,}$/.test(e);
const normPh=x=>{let s=String(x||'').replace(/[\s\-()]/g,'');if(/^01\d{9}$/.test(s))s='+88'+s;else if(/^8801\d{9}$/.test(s))s='+'+s;else if(/^00/.test(s))s='+'+s.slice(2);return/^\+[1-9]\d{7,14}$/.test(s)?s:null};
async function sendOtp(type,to,code){const msg='Bondhu verification code: '+code+' (10 minute valid. Eta kauke dio na)',sig=AbortSignal.timeout(15000);
 if(type=='email'){const f=process.env.MAIL_FROM,sub='Bondhu verification code';
  if(process.env.BREVO_API_KEY){const m=f.match(/^(.*)<(.+)>\s*$/),sender=m?{name:m[1].trim()||'Bondhu',email:m[2].trim()}:{name:'Bondhu',email:f.trim()};
   const rs=await fetch('https://api.brevo.com/v3/smtp/email',{method:'POST',headers:{'api-key':process.env.BREVO_API_KEY,'Content-Type':'application/json',accept:'application/json'},body:JSON.stringify({sender,to:[{email:to}],subject:sub,textContent:msg}),signal:sig});
   if(!rs.ok)throw new Error('Brevo HTTP '+rs.status);return}
  const rs=await fetch('https://api.resend.com/emails',{method:'POST',headers:{Authorization:'Bearer '+process.env.RESEND_API_KEY,'Content-Type':'application/json'},body:JSON.stringify({from:f,to:[to],subject:sub,text:msg}),signal:sig});
  if(!rs.ok)throw new Error('Resend HTTP '+rs.status);return}
 const sid=process.env.TWILIO_SID,fr=process.env.TWILIO_FROM,b=new URLSearchParams({To:to,Body:msg});b.set(/^MG/.test(fr)?'MessagingServiceSid':'From',fr);
 const rs=await fetch('https://api.twilio.com/2010-04-01/Accounts/'+encodeURIComponent(sid)+'/Messages.json',{method:'POST',headers:{Authorization:'Basic '+Buffer.from(sid+':'+process.env.TWILIO_TOKEN).toString('base64'),'Content-Type':'application/x-www-form-urlencoded'},body:b,signal:sig});
 if(!rs.ok)throw new Error('Twilio HTTP '+rs.status)}
app.post('/api/verify/send',auth,async(q,r)=>{const type=q.body.type=='phone'?'phone':'email',raw=String(q.body.value||'').trim(),f=type=='email'?'em':'ph',nm=type=='email'?'Email':'Phone number';
 const val=type=='email'?(emailOk(raw.toLowerCase())?raw.toLowerCase():null):normPh(raw);
 if(!val)return r.status(400).json({e:type=='email'?'Email thik na':'Phone number thik na (01XXXXXXXXX ba +countrycode)'});
 const can=type=='email'?mailOn():smsOn();if(!can&&!devOtp())return r.status(503).json({e:'Ei server e '+(type=='email'?'email':'SMS')+' OTP service chalu nai. Admin ke setup korte bolo.'});
 if(q.u[f]==val)return r.status(400).json({e:nm+' already verified'});
 if(D.users.some(u=>u.id!=q.me&&u[f]==val))return r.status(400).json({e:'Ei '+nm+' onno account e verified ache'});
 const o=OTP.get(q.me),w=o?Math.ceil((6e4-(Date.now()-o.sent))/1000):0;if(w>0)return r.status(429).json({e:'Aro '+w+' second pore abar pathao',wait:w});
 if(!hit('os'+q.me,5,36e5)||!hit('oc'+type+val,3,36e5))return r.status(429).json({e:'Onek bar OTP chawa hoyeche, 1 ghonta pore try koro'});
 const code=String(crypto.randomInt(0,1e6)).padStart(6,'0'),salt=crypto.randomBytes(8).toString('hex');
 try{if(can)await sendOtp(type,val,code);else console.log('[OTP_DEV]',type,val,code)}catch(e){console.log('OTP send fail:',String(e.message||e).slice(0,120));return r.status(502).json({e:'OTP pathano jayni. Admin ke provider/key check korte bolo.'})}
 OTP.set(q.me,{type,val,salt,h:hash(code,salt),exp:Date.now()+6e5,tries:0,sent:Date.now()});r.json({ok:1,wait:60})});
app.post('/api/verify/confirm',auth,(q,r)=>{const o=OTP.get(q.me),c=String(q.body.code||'').trim();
 if(!o||Date.now()>o.exp){OTP.delete(q.me);return r.status(400).json({e:'OTP nai ba expire hoye geche, abar pathao'})}
 if(!/^\d{6}$/.test(c))return r.status(400).json({e:'6 digit er code dao'});
 if(!crypto.timingSafeEqual(Buffer.from(hash(c,o.salt)),Buffer.from(o.h))){o.tries++;if(o.tries>=5){OTP.delete(q.me);return r.status(429).json({e:'Onek bar vul. Notun OTP nao'})}return r.status(400).json({e:'OTP vul ('+(5-o.tries)+' bar baki)'})}
 const f=o.type=='email'?'em':'ph';
 if(D.users.some(u=>u.id!=q.me&&u[f]==o.val)){OTP.delete(q.me);return r.status(400).json({e:'Ei '+(f=='em'?'email':'number')+' onno account e verified hoye geche'})}
 q.u[f]=o.val;q.u.vf=1;OTP.delete(q.me);save();io.emit('sync');r.json({ok:1})});
app.post('/api/verify/remove',auth,(q,r)=>{if(q.u.h!=hash(String(q.body.pw||''),q.u.salt))return r.status(400).json({e:'Password vul'});
 delete q.u[q.body.type=='phone'?'ph':'em'];if(q.u.em||q.u.ph)q.u.vf=1;else delete q.u.vf;save();io.emit('sync');r.json({ok:1})});


/* ---- Stories (24h), message delete/react, link preview ---- */
const inUse=(url,ex)=>D.posts.some(p=>p.vid==url)||D.msgs.some(x=>x.m&&x.m.u==url)||D.users.some(u=>u.pic==url)||D.stories.some(s=>s!==ex&&s.url==url);
function rmStory(s){D.stories=D.stories.filter(x=>x!==s);if(!inUse(s.url,s))unf(s.url)}
function purgeStories(){const old=D.stories.filter(s=>Date.now()-s.t>=SD);if(old.length){old.forEach(rmStory);save();io.emit('sync')}}
purgeStories();setInterval(purgeStories,6e5).unref();
app.post('/api/story',auth,(q,r)=>{const mu=String((q.body.m||{}).url||''),cap=String(q.body.cap||'').trim().slice(0,200);
 if(!MEDIA.test(mu))return r.status(400).json({e:'Photo ba video dao'});
 if(D.stories.filter(s=>s.u==q.me).length>=20)return r.status(400).json({e:'Ek diner moddhe max 20 ta story'});
 D.stories.push({id:uid(),u:q.me,url:mu,k:mu.endsWith('.jpg')?'img':'vid',cap,t:Date.now(),v:[]});save();io.emit('sync');r.json({ok:1})});
app.post('/api/story/:id/del',auth,(q,r)=>{const s=D.stories.find(x=>x.id==q.params.id);if(s&&(s.u==q.me||q.u.admin)){rmStory(s);save();io.emit('sync')}r.json({ok:1})});
app.post('/api/story/:id/seen',auth,(q,r)=>{const s=D.stories.find(x=>x.id==q.params.id);
 if(s&&s.u!=q.me&&isF(q.me,s.u)&&!blocked(q.me,s.u)&&!s.v.includes(q.me)){s.v.push(q.me);save();ping(s.u)}r.json({ok:1})});
const MRX=['love','like','dislike'];
app.post('/api/m/:mid/del',auth,(q,r)=>{const x=D.msgs.find(y=>y.id==q.params.mid);
 if(x&&(x.a==q.me||x.b==q.me)){if(x.a==q.me){D.msgs=D.msgs.filter(y=>y!==x);if(x.m&&!inUse(x.m.u))unf(x.m.u)}else{x.hd=x.hd||[];if(!x.hd.includes(q.me))x.hd.push(q.me)}save();ping(x.a,x.b)}r.json({ok:1})});
app.post('/api/m/:mid/react/:k',auth,(q,r)=>{const x=D.msgs.find(y=>y.id==q.params.mid),k=q.params.k;
 if(x&&(x.a==q.me||x.b==q.me)&&MRX.includes(k)&&!blocked(x.a,x.b)){x.rx=x.rx||{};if(x.rx[q.me]==k)delete x.rx[q.me];else x.rx[q.me]=k;save();ping(x.a,x.b)}r.json({ok:1})});

const privIp=ip=>{if(net.isIPv6(ip)){const l=ip.toLowerCase();if(l=='::1'||l=='::'||/^f[cd]/.test(l)||/^fe[89ab]/.test(l))return true;const m=l.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);return m?privIp(m[1]):false}
 const p=ip.split('.').map(Number);return p[0]==10||p[0]==127||p[0]==0||(p[0]==169&&p[1]==254)||(p[0]==172&&p[1]>=16&&p[1]<=31)||(p[0]==192&&p[1]==168)||(p[0]==100&&p[1]>=64&&p[1]<=127)||p[0]>=224};
function safeLookup(host,o,cb){dns.lookup(host,{all:true},(e,a)=>{if(e)return cb(e);const ok=a.filter(x=>!privIp(x.address));if(!ok.length)return cb(new Error('blocked'));if(o&&o.all)return cb(null,ok);cb(null,ok[0].address,ok[0].family)})}
function getHtml(u,hop,done){let x;try{x=new URL(u)}catch(e){return done(new Error('url'))}
 if(!/^https?:$/.test(x.protocol)||(net.isIP(x.hostname.replace(/^\[|\]$/g,''))&&privIp(x.hostname.replace(/^\[|\]$/g,''))))return done(new Error('blocked'));
 const lib=x.protocol=='https:'?https:http;let fin=false;const end=(e,v)=>{if(!fin){fin=true;done(e,v)}};
 const rq=lib.get(x,{lookup:safeLookup,timeout:6000,headers:{'User-Agent':'Mozilla/5.0 (compatible; BondhuLinkPreview/1.0)','Accept':'text/html,application/xhtml+xml','Accept-Language':'en,bn;q=0.8','Accept-Encoding':'gzip, deflate'}},res=>{
  if(res.statusCode>=300&&res.statusCode<400&&res.headers.location){res.resume();if(hop>=3)return end(new Error('redirects'));return getHtml(new URL(res.headers.location,x).href,hop+1,end)}
  if(res.statusCode!=200||!/html/i.test(res.headers['content-type']||'')){res.resume();return end(new Error('type'))}
  let src=res;const ce=res.headers['content-encoding'];if(ce=='gzip')src=res.pipe(zlib.createGunzip());else if(ce=='deflate')src=res.pipe(zlib.createInflate());
  const ch=[];let n=0;src.on('data',c=>{n+=c.length;ch.push(c);if(n>6e5){rq.destroy();end(null,{h:Buffer.concat(ch).toString('utf8'),u:x.href})}});
  src.on('end',()=>end(null,{h:Buffer.concat(ch).toString('utf8'),u:x.href}));src.on('error',end)});
 rq.on('timeout',()=>{rq.destroy();end(new Error('timeout'))});rq.on('error',end)}
const hdec=t=>String(t||'').replace(/&amp;/g,'&').replace(/&quot;/g,'"').replace(/&#0?39;|&apos;/g,"'").replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#(\d+);/g,(_,n)=>String.fromCodePoint(+n)).trim();
function parseMeta(h,base){const M={};(h.match(/<meta\b[^>]*>/gi)||[]).forEach(t=>{const a={};t.replace(/([a-zA-Z:_-]+)\s*=\s*(?:"([^"]*)"|'([^']*)')/g,(_,k,v1,v2)=>{a[k.toLowerCase()]=v1!==undefined?v1:v2});const k=(a.property||a.name||'').toLowerCase();if(k&&a.content!==undefined&&!M[k])M[k]=hdec(a.content)});
 const ti=(h.match(/<title[^>]*>([\s\S]*?)<\/title>/i)||[])[1];let img=M['og:image']||M['og:image:url']||M['twitter:image']||M['twitter:image:src']||'';
 try{img=img?new URL(img,base).href:''}catch(e){img=''}if(!/^https?:\/\//.test(img))img='';
 return{title:(M['og:title']||M['twitter:title']||hdec(ti)).slice(0,160),desc:(M['og:description']||M['twitter:description']||M.description||'').slice(0,240),img,site:(M['og:site_name']||new URL(base).hostname.replace(/^www\./,'')).slice(0,60)}}
const PV=new Map();
app.get('/api/preview',auth,(q,r)=>{const u=String(q.query.url||'').slice(0,500);if(!/^https?:\/\//i.test(u))return r.status(400).json({e:'url'});
 if(!hit('pv'+q.me,40,6e4))return r.status(429).json({e:'slow'});
 const c=PV.get(u);if(c&&Date.now()-c.t<(c.v?36e5:6e5))return r.json(c.v||{});
 getHtml(u,0,(e,x)=>{let v=null;if(!e){try{v=parseMeta(x.h,x.u);if(!v.title&&!v.img)v=null}catch(_){v=null}}
  PV.set(u,{t:Date.now(),v});if(PV.size>300)PV.delete(PV.keys().next().value);r.json(v||{})})});

/* ---- ICE (call relay), push, pages ---- */
let iceC=null,iceErr='';
async function ice(){const base=[{urls:['stun:stun.l.google.com:19302','stun:stun1.l.google.com:19302']}];
 if(iceC&&Date.now()-iceC.t<18e5)return iceC.v;let v=base;
 try{if(process.env.METERED_DOMAIN&&process.env.METERED_API_KEY){
   const h=process.env.METERED_DOMAIN.replace(/^https?:\/\//,'').replace(/\/.*/,''),rs=await fetch('https://'+h+'/api/v1/turn/credentials?apiKey='+encodeURIComponent(process.env.METERED_API_KEY),{signal:AbortSignal.timeout(8000)});
   if(rs.ok){const j=await rs.json();if(Array.isArray(j)&&j.length){v=base.concat(j);iceErr=''}else iceErr='Metered khali list dilo'}else iceErr='Metered HTTP '+rs.status+' (key/domain check koro)'}
  else if(process.env.TURN_URLS)v=base.concat({urls:process.env.TURN_URLS.split(',').map(x=>x.trim()).filter(Boolean),username:process.env.TURN_USER||'',credential:process.env.TURN_PASS||''})}catch(e){iceErr=String(e.message||e).slice(0,100)}
 iceC={t:v===base?Date.now()-17e5:Date.now(),v};return v}
app.get('/api/ice',auth,async(q,r)=>{const v=await ice();r.json({s:v,turn:v.length>1})});
app.get('/api/push/key',auth,(q,r)=>r.json({k:wp?VP.pub:null}));
app.post('/api/push/sub',auth,(q,r)=>{const s=q.body.sub;
 if(!wp||!s||typeof s.endpoint!='string'||!/^https:\/\//.test(s.endpoint)||!s.keys)return r.status(400).json({e:'Push support nai'});
 D.ps=D.ps.filter(x=>x.sub.endpoint!=s.endpoint);D.ps.push({u:q.me,sub:{endpoint:s.endpoint,keys:s.keys}});
 const mine=D.ps.filter(x=>x.u==q.me);if(mine.length>5)D.ps=D.ps.filter(x=>!mine.slice(0,mine.length-5).includes(x));save();r.json({ok:1})});
app.post('/api/push/unsub',auth,(q,r)=>{D.ps=D.ps.filter(x=>!(x.u==q.me&&x.sub.endpoint==q.body.ep));save();r.json({ok:1})});
app.post('/api/pg/:id/:v',auth,(q,r)=>{const g=D.pages.find(x=>x.id==q.params.id);if(!g)return r.status(400).json({e:'Page nai'});
 D.pf=D.pf.filter(x=>!(x.u==q.me&&x.pg==g.id));if(q.params.v=='1')D.pf.push({u:q.me,pg:g.id});save();io.emit('sync');r.json({ok:1})});

/* RSS -> page posts (title + short summary + link only) */
const dec=x=>x.replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g,'$1').replace(/&(#x[0-9a-f]+|#\d+|amp|lt|gt|quot|apos|nbsp);/gi,(m,e)=>{e=e.toLowerCase();
  if(e[0]=='#'){const c=e[1]=='x'?parseInt(e.slice(2),16):+e.slice(1);try{return String.fromCodePoint(c)}catch(_){return ''}}return({amp:'&',lt:'<',gt:'>',quot:'"',apos:"'",nbsp:' '})[e]}).replace(/<[^>]+>/g,' ').replace(/\s+/g,' ').trim();
const tag=(x,n)=>{const m=x.match(new RegExp('<'+n+'(?:\\s[^>]*)?>([\\s\\S]*?)</'+n+'>','i'));return m?m[1]:''};
function parseFeed(x){return(x.match(/<(item|entry)[\s>][\s\S]*?<\/\1>/gi)||[]).slice(0,15).map(i=>{
 let link=dec(tag(i,'link'));if(!link){const m=i.match(/<link[^>]*href=["']([^"']+)["']/i);link=m?m[1]:''}
 const pd=Date.parse(dec(tag(i,'pubDate')||tag(i,'published')||tag(i,'updated')));
 return{title:dec(tag(i,'title')).slice(0,300),link:link.trim(),desc:dec(tag(i,'description')||tag(i,'summary')).slice(0,240),t:isNaN(pd)?Date.now():Math.min(pd,Date.now())}}).filter(i=>i.title&&/^https?:\/\//.test(i.link))}
async function pull(g){if(!g.rss)return;
 try{const rs=await fetch(g.rss,{headers:{'User-Agent':'Mozilla/5.0 (compatible; BondhuBot/1.0)',Accept:'application/rss+xml,application/xml,text/xml,*/*'},signal:AbortSignal.timeout(15000)});
  if(!rs.ok)throw new Error('HTTP '+rs.status);const items=parseFeed((await rs.text()).slice(0,2e6));if(!items.length)throw new Error('Kono item pawa jayni (RSS link thik?)');
  let n=0;items.reverse().forEach(i=>{if(D.posts.some(p=>p.pg==g.id&&p.link==i.link))return;
   D.posts.push({id:uid(),uid:g.id,pg:g.id,text:i.title,desc:i.desc,link:i.link,vis:'pub',tags:[],allow:[],t:i.t,rx:{},c:[]});n++});
  const old=new Set(D.posts.filter(p=>p.pg==g.id).sort((a,b)=>a.t-b.t).slice(0,-15));if(old.size)D.posts=D.posts.filter(p=>!old.has(p));
  g.ok=Date.now();g.err=null;save();if(n)io.emit('sync')}
 catch(e){g.err=String(e.message||e).slice(0,120);save()}}
const pullAll=async()=>{for(const g of D.pages.slice())await pull(g)};
setTimeout(pullAll,5000);setInterval(pullAll,20*60*1000);

app.get('/api/admin',adm,async(q,r)=>{await ice();const nm=id=>(D.users.find(u=>u.id==id)||D.pages.find(g=>g.id==id)||{name:'?'}).name;
 r.json({max:MAX(),turn:!!((process.env.METERED_DOMAIN&&process.env.METERED_API_KEY)||process.env.TURN_URLS),push:!!wp,mail:mailOn(),sms:smsOn(),turnGot:!!(iceC&&iceC.v.length>1),iceErr,pages:D.pages.map(g=>({id:g.id,name:g.name,rss:g.rss||'',ver:!!g.ver,ok:g.ok||0,err:g.err||''})),posts:D.posts.filter(p=>!p.del).length,
 users:D.users.map(u=>({id:u.id,name:u.name,admin:!!u.admin,ban:!!u.ban,posts:D.posts.filter(p=>p.uid==u.id&&!p.del).length})),
 reports:D.rp.map(x=>{const p=x.type=='post'?D.posts.find(p=>p.id==x.tid):null;if(x.type=='post'&&!p)return null;const au=p?p.uid:x.tid;
  return{id:x.id,type:x.type,by:nm(x.by),target:nm(au),uid:au,tid:x.tid,reason:x.reason,snip:p?(p.text||'(photo/video)').slice(0,120):''}}).filter(Boolean)})});
app.post('/api/admin/ban/:id/:v',adm,(q,r)=>{const u=D.users.find(x=>x.id==q.params.id);
 if(u&&!u.admin){u.ban=q.params.v=='1';if(u.ban){for(const k in D.tok)if(D.tok[k]==u.id)delete D.tok[k];io.in('u'+u.id).disconnectSockets(true)}save();io.emit('sync')}r.json({ok:1})});
app.post('/api/admin/del/:id',adm,(q,r)=>{const u=D.users.find(x=>x.id==q.params.id);if(u&&!u.admin)rmUser(u.id);r.json({ok:1})});
app.post('/api/admin/post/:id',adm,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id);if(p)rmPost(p);r.json({ok:1})});
app.post('/api/admin/rp/:id',adm,(q,r)=>{D.rp=D.rp.filter(x=>x.id!=q.params.id);save();ping(q.me);r.json({ok:1})});
app.post('/api/admin/max',adm,(q,r)=>{const n=Math.floor(+q.body.n);if(n>=2&&n<=1000){D.cfg.max=n;save()}ping(q.me);r.json({ok:1})});

const okUrl=u=>/^https?:\/\/[^\s]+$/.test(u);
app.post('/api/admin/pg',adm,(q,r)=>{const name=String(q.body.name||'').trim().slice(0,40),rss=String(q.body.rss||'').trim();
 if(!name||(rss&&!okUrl(rss)))return r.status(400).json({e:'Naam dao, RSS link http(s) diye hote hobe'});
 const g={id:uid(),name,rss,ver:q.body.ver?1:0};D.pages.push(g);save();io.emit('sync');pull(g);r.json({ok:1})});
app.post('/api/admin/pg/:id/del',adm,(q,r)=>{const g=D.pages.find(x=>x.id==q.params.id);if(g){const ids=new Set(D.posts.filter(p=>p.pg==g.id).map(p=>p.id));
 D.posts=D.posts.filter(p=>p.pg!=g.id);D.rp=D.rp.filter(x=>!(x.type=='post'&&ids.has(x.tid)));D.pf=D.pf.filter(x=>x.pg!=g.id);D.pages=D.pages.filter(x=>x!==g);save();io.emit('sync')}r.json({ok:1})});
app.post('/api/admin/pg/:id/pull',adm,async(q,r)=>{const g=D.pages.find(x=>x.id==q.params.id);if(g)await pull(g);io.emit('sync');r.json({ok:1})});
app.post('/api/admin/pg/:id/post',adm,(q,r)=>{const g=D.pages.find(x=>x.id==q.params.id),text=String(q.body.text||'').trim().slice(0,2000),link=String(q.body.link||'').trim();
 if(!g||!text||(link&&!okUrl(link)))return r.status(400).json({e:'Text dao, link http(s) hote hobe'});
 D.posts.push({id:uid(),uid:g.id,pg:g.id,text,desc:'',link,vis:'pub',tags:[],allow:[],t:Date.now(),rx:{},c:[]});save();io.emit('sync');r.json({ok:1})});
app.get('/api/admin/backup',adm,(q,r)=>{r.setHeader('Content-Type','application/json');r.setHeader('Content-Disposition','attachment; filename="bondhu-backup.json"');r.send(JSON.stringify(D))});
app.post('/api/admin/restore',adm,express.text({type:'text/plain',limit:'100mb'}),(q,r)=>{let j;try{j=JSON.parse(q.body)}catch(e){}
 if(!j||!Array.isArray(j.users)||!Array.isArray(j.posts))return r.status(400).json({e:'Eta valid backup file na'});
 const n=Object.assign({users:[],posts:[],fr:[],rq:[],msgs:[],tok:{},bl:[],mt:[],nt:[],rp:[],cfg:{},pages:[],pf:[],ps:[],stories:[]},j);
 Object.keys(D).forEach(k=>delete D[k]);Object.assign(D,n);iceC=null;initPush();save();io.emit('sync');r.json({ok:1})});

io.use((s,n)=>{const id=D.tok[s.handshake.auth.token],u=D.users.find(x=>x.id==id);if(!u||u.ban)return n(new Error('auth'));s.uid=id;n()});
const pend=new Map();
io.on('connection',s=>{s.join('u'+s.uid);on.set(s.uid,(on.get(s.uid)||0)+1);io.emit('sync');
 if(pend.has(s.uid)){const p=pend.get(s.uid);pend.delete(s.uid);if(Date.now()-p.t<45e3)p.m.forEach(d=>s.emit('sig',{from:p.from,data:d}))}
 s.on('sig',({to,data})=>{if(!data||!canTalk(s.uid,to))return;
  if(data.t=='offer')pushTo(to,{title:nameOf(s.uid)+' tomake '+(data.v?'video':'voice')+' call korche',body:'App khulo, call accept korte',tag:'call'+s.uid});
  if(on.has(to))return io.to('u'+to).emit('sig',{from:s.uid,data});
  if(data.t=='offer')pend.set(to,{from:s.uid,t:Date.now(),m:[data]});
  else{const p=pend.get(to);if(p&&p.from==s.uid){if(data.t=='bye')pend.delete(to);else if(p.m.length<60)p.m.push(data)}}});
 s.on('disconnect',()=>{const c=(on.get(s.uid)||1)-1;c?on.set(s.uid,c):on.delete(s.uid);io.emit('sync')})});
srv.listen(process.env.PORT||3000,()=>console.log('Bondhu running'));
}
