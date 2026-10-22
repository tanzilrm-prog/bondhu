const express=require('express'),http=require('http'),{Server}=require('socket.io'),crypto=require('crypto'),fs=require('fs'),path=require('path');
const app=express(),srv=http.createServer(app),io=new Server(srv);
const F=process.env.DATA_FILE||path.join(__dirname,'data.json');
let D={users:[],posts:[],fr:[],rq:[],msgs:[],tok:{}};
try{D=Object.assign(D,JSON.parse(fs.readFileSync(F,'utf8')))}catch(e){}
let st;const save=()=>{clearTimeout(st);st=setTimeout(()=>fs.writeFile(F,JSON.stringify(D),()=>{}),300)};
const uid=()=>Date.now()*1000+(Math.random()*1000|0);
const hash=(p,s)=>crypto.scryptSync(p,s,32).toString('hex');
const isF=(a,b)=>D.fr.some(f=>f.includes(a)&&f.includes(b));
const on=new Map(),ping=(...ids)=>ids.forEach(i=>io.to('u'+i).emit('sync'));

app.use(express.json({limit:'4mb'}));
app.use(express.static(path.join(__dirname,'public')));
const auth=(q,r,n)=>{const id=D.tok[(q.headers.authorization||'').slice(7)];if(!id)return r.status(401).json({e:'Login koro'});q.me=id;n()};
const pub=u=>({id:u.id,name:u.name});
function login(u,r){const tk=crypto.randomBytes(24).toString('hex');D.tok[tk]=u.id;save();r.json({token:tk})}

app.post('/api/signup',(q,r)=>{const n=String(q.body.name||'').trim().slice(0,30),p=String(q.body.pw||'');
 if(!n||p.length<4)return r.status(400).json({e:'Naam dao, password minimum 4 character'});
 if(D.users.some(u=>u.name.toLowerCase()==n.toLowerCase()))return r.status(400).json({e:'Ei naam ache'});
 const salt=crypto.randomBytes(8).toString('hex'),u={id:uid(),name:n,salt,h:hash(p,salt)};D.users.push(u);io.emit('sync');login(u,r)});
app.post('/api/login',(q,r)=>{const u=D.users.find(u=>u.name.toLowerCase()==String(q.body.name||'').trim().toLowerCase());
 if(!u||u.h!=hash(String(q.body.pw||''),u.salt))return r.status(400).json({e:'Vul naam ba password'});login(u,r)});

app.get('/api/state',auth,(q,r)=>{const m=q.me;r.json({me:m,users:D.users.map(pub),posts:D.posts.slice(-100),
 fr:D.fr.filter(f=>f.includes(m)),rq:D.rq.filter(x=>x.from==m||x.to==m),
 msgs:D.msgs.filter(x=>x.a==m||x.b==m).slice(-500),online:[...on.keys()]})});

app.post('/api/post',auth,(q,r)=>{const text=String(q.body.text||'').slice(0,2000),
 img=typeof q.body.img=='string'&&q.body.img.startsWith('data:image/')?q.body.img:null;
 if(!text&&!img)return r.status(400).json({e:'Khali post'});
 D.posts.push({id:uid(),uid:q.me,text,img,t:Date.now(),likes:[],c:[]});save();io.emit('sync');r.json({ok:1})});
app.post('/api/like/:id',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id);
 if(p){const i=p.likes.indexOf(q.me);i<0?p.likes.push(q.me):p.likes.splice(i,1);save();io.emit('sync')}r.json({ok:1})});
app.post('/api/comment/:id',auth,(q,r)=>{const p=D.posts.find(x=>x.id==q.params.id),text=String(q.body.text||'').slice(0,500);
 if(p&&text){p.c.push({uid:q.me,text});save();io.emit('sync')}r.json({ok:1})});

app.post('/api/fr/:id/:a',auth,(q,r)=>{const m=q.me,o=+q.params.id,a=q.params.a;
 if(!D.users.some(u=>u.id==o)||o==m)return r.status(400).json({e:'Vul user'});
 const had=D.rq.some(x=>x.from==o&&x.to==m),mine=D.rq.some(x=>x.from==m&&x.to==o),fr=isF(m,o);
 D.rq=D.rq.filter(x=>!(x.from==m&&x.to==o)&&!(x.from==o&&x.to==m));
 if(a=='acc'&&had)D.fr.push([m,o]);
 else if(a=='rm')D.fr=D.fr.filter(f=>!(f.includes(m)&&f.includes(o)));
 else if(a=='add'&&!fr&&!had&&!mine)D.rq.push({from:m,to:o});
 else if(a=='rej'){}
 save();ping(m,o);r.json({ok:1})});

app.post('/api/msg/:id',auth,(q,r)=>{const o=+q.params.id,text=String(q.body.text||'').slice(0,2000);
 if(!text||!isF(q.me,o))return r.status(400).json({e:'Age bondhu hou'});
 D.msgs.push({id:uid(),a:q.me,b:o,text,t:Date.now()});save();ping(q.me,o);r.json({ok:1})});

io.use((s,n)=>{const id=D.tok[s.handshake.auth.token];if(!id)return n(new Error('auth'));s.uid=id;n()});
io.on('connection',s=>{s.join('u'+s.uid);on.set(s.uid,(on.get(s.uid)||0)+1);io.emit('sync');
 s.on('sig',({to,data})=>{if(isF(s.uid,to))io.to('u'+to).emit('sig',{from:s.uid,data})});
 s.on('disconnect',()=>{const c=(on.get(s.uid)||1)-1;c?on.set(s.uid,c):on.delete(s.uid);io.emit('sync')})});

srv.listen(process.env.PORT||3000,()=>console.log('Bondhu running'));
