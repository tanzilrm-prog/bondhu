self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(clients.claim()));
self.addEventListener('push',e=>{let d={};try{d=e.data.json()}catch(_){}
 e.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(cs=>{
  if(cs.some(c=>c.visibilityState==='visible'))return;
  return self.registration.showNotification(d.title||'Bondhu',{body:d.body||'',tag:d.tag||undefined,renotify:!!d.tag,icon:'/icon-192.png',badge:'/icon-192.png'})}))});
self.addEventListener('notificationclick',e=>{e.notification.close();
 e.waitUntil(clients.matchAll({type:'window',includeUncontrolled:true}).then(cs=>cs.length?cs[0].focus():clients.openWindow('/')))});
