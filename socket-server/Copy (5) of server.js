import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import axios from 'axios';

//const cors = require("cors");
const app = express();
const server = createServer(app);
const io = new Server(server, { 
	cors: { 
		origin: '*',	
		//methods: ['GET', 'POST'],
	//    credentials: true,
          transports: ['websocket', 'polling']  
	},	
	path: "/gellybook/",
	//maxHttpBufferSize: 1e8, // 100 MB ,
	//allowEIO3: true
	pingInterval: 5000, // كل 10 ثواني
	  pingTimeout: 10000   // لو مردش خلال 20 ثانية = disconnect

 });
 // لتخزين المستخدمين المتصلين: userId -> Set من socket.id
 const onlineUsers = new Map();

 // دورة زمنية للبث (مثلاً 10 ثوانٍ)
 const ONLINE_BROADCAST_INTERVAL = 10000; // 10 seconds
 //app.use(cors());
const redis = new Redis();
let openChats = {};
redis.psubscribe('*');
const lastActivity = new Map();
//redis.on('pmessage', (_, __, message) => {
//    const payload = JSON.parse(message);
//    const { sender_id, receiver_id } = payload.data;
//
//    io.to(`chat.${sender_id}`).emit(payload.event, payload.data);
//    io.to(`chat.${receiver_id}`).emit(payload.event, payload.data); 
//});
const gellybookns = io.of("/gellybook");

redis.on('pmessage', (pattern, channel, message) => {
//	console.log('CHANNEL:', channel);
	//  console.log('RAW MESSAGE:', message);
    const payload = JSON.parse(message);
    if (!payload?.data) return;

    const event = payload.event;
    const data = payload.data;
	if (event === 'message.sent' || event === 'message.deleted' ) {
	       const senderRoom = 'chat.' + payload.data.sender_id;
	       const receiverRoom = 'chat.' + payload.data.receiver_id;

	       gellybookns.to(senderRoom).emit(event, payload.data);
	       gellybookns.to(receiverRoom).emit(event, payload.data);
	   }
 else if (event === 'message.seen' || event === 'message.delivered' ) {
        // هنا نرسل فقط حالة "تمت القراءة" لكل من المرسل والمستقبل
        const msg = data.message;
        const messageId = data.message_id;
        const status = data.status;

        if (!msg || !messageId || !status) return;

        const senderRoom = 'chat.' + msg.member_id; // مرسل الرسالة
        const receiverRoom = 'chat.' + (data.receiver_id || msg.member_id); // المستقبل

        gellybookns.to(senderRoom).emit(event, data);
        gellybookns.to(receiverRoom).emit(event, data);
    }
	// 👇 إضافة دعم الجروب
	else if(event === 'message.sent.group' || event === 'message.deleted.group') {
	    const groupRoom = 'group.' + payload.data.group_id;
		console.log(groupRoom + " group ");
	    gellybookns.to(groupRoom).emit(event, payload.data);
	}
	else if(event ===  'post.newpost'){
		console.log("message.newpost");
		       const receiverRoom = 'newpost.' + payload.data.receiver_id;
 		       gellybookns.to(receiverRoom).emit(event, payload.data);
	}
	else if(event ===  'message.friendrequestsent'){
		console.log("message.friendrequestsent "+payload.data.receiver_id);
	//	console.log("message.friendrequestsent "+payload);
					
			       const receiverRoom = 'friendrequestsent.' + payload.data.receiver_id;
	 		       gellybookns.to(receiverRoom).emit(event, payload.data);
		}
		else if(event ===  'message.friendrequestcanceled'){
				console.log("message.friendrequestcanceled "+payload.data.receiver_id);
				//console.log("message.friendrequestcanceled "+payload);
							
					       const receiverRoom = 'friendrequestcanceled.' + payload.data.receiver_id;
			 		       gellybookns.to(receiverRoom).emit(event, payload.data);
				}
});
/*setInterval(() => {

const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', {
        users: onlineList
    });

}, 10000);*/

function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', {
        users: onlineList
    });
}

// بعد كده ممكن تستخدمها في setInterval
setInterval(() => {
/*  
// علشان يعمل اوفلاين اوتوماتيك بعد مده 30 ثانية مثلا
  const now = Date.now();
    for (const [userId, sockets] of onlineUsers.entries()) {
        const last = lastActivity.get(userId) || 0;
        if (now - last > 30000) { // 30 ثانية inactivity
            onlineUsers.delete(userId);
            lastActivity.delete(userId);
        }
    }*/
    broadcastOnlineUsers();
}, 10000);
   gellybookns.on('connection', socket => {
	const token = socket.handshake.auth.token;
	    const userId = socket.handshake.auth.userId;

	    socket.userId = userId;
	    socket.token = token;
	 // لما يكون فيه اكتر من ايفنت مثلا نستخدم ايفين معين مرتين او تلاتة او اكتر يسبب مشكلة و تظهر الرسالة التالية
	//(node:20616) MaxListenersExceededWarning: Possible EventEmitter memory leak detected. 11 useristyping listeners added to [Socket]. MaxListeners is 10. Use emitter.setMaxListeners() to increase limit
	// الحل نشل الكومنت نستخدم قيمة لعدد الاحداث المتكررة او نخليها ب 0 اي عدد غير محدود
	//	socket.setMaxListeners(20); // Set a higher limit
   // مثلا نستخدم useristyping اكتر من مرة مع بعض
	socket.on('join-profile', userId => {
	   socket.join(`profile.${userId}`)
	 })
    socket.on('join', (room) =>{
		
		socket.join(room);
	//	console.log(socket.id + " joined " + room);
	const userId = String(room).split('.').pop();
	if (!openChats[userId]) openChats[userId] = [];
	lastActivity.set(userId, Date.now());

		   socket.userId = userId;

	   if (!onlineUsers.has(userId)) {
	       onlineUsers.set(userId, new Set());
	   }

	   onlineUsers.get(userId).add(socket.id);
	   broadcastOnlineUsers();
/*		socket.userId = room;
			const userId=room;
			if (!openChats[userId]) openChats[userId] = [];*/
	});
	//socket.removeListener('useristyping',({ sender,receiver})=>{});
	socket.on("useristyping",({ sender,receiver})=>{
		lastActivity.set(socket.userId, Date.now());
		console.log("typing : "+sender +" + " + receiver);
		const senderRoom = 'chat.' + sender;
		const receiverRoom = 'chat.' +receiver;
		gellybookns.to(senderRoom).emit('useristyping',{sender:sender,receiver:receiver });
		gellybookns.to(receiverRoom).emit('useristyping',{sender:sender,receiver:receiver });

	})
	socket.on('get-last-seen', async (userId, callback) => {
	    try {
	        const sockets = onlineUsers.get(String(userId));
	        if (sockets && sockets.size > 0) {
	            // المستخدم أونلاين الآن
	            callback({ lastSeen: 'online' });
	        } else {
	            // مستخدم أوفلاين، خد آخر ظهور من قاعدة البيانات أو من lastActivity
	            const lastSeen = lastActivity.get(String(userId)) || Date.now();
	            callback({ lastSeen });
	        }
	    } catch (err) {
	        console.error(err);
	        callback({ lastSeen: null });
	    }
	});
		socket.on('chat.opened', ({ chatWith }) => {
	    const userId = socket.userId;

	    if (!openChats[userId].includes(chatWith)) {
	        openChats[userId].push(chatWith);
	    }

	    // افحص الرسائل اللي لسه مش مقروءة
	    chatMessages
	        .filter(msg => msg.sender_id === chatWith && msg.receiver_id === userId && msg.status !== 'seen')
	        .forEach(msg => {
	            msg.status = 'seen';

	            // ابعت للمرسل وال المستقبل
	            const senderRoom = 'chat.' + msg.sender_id;
	            const receiverRoom = 'chat.' + msg.receiver_id;

	            gellybookns.to(senderRoom).emit('message.seen', { message_id: msg.id, status: 'seen' });
	            gellybookns.to(receiverRoom).emit('message.seen', { message_id: msg.id, status: 'seen' });
	        });
	});


	   socket.on('chat.closed', ({ chatWith }) => {
	       openChats[socket.userId] = openChats[socket.userId].filter(id => id !== chatWith);
	   });
	   socket.on('heartbeat', () => {
	       if (socket.userId) {
	           lastActivity.set(socket.userId, Date.now());
	       }
	   });
	   socket.on("updateLastSeen", async () => {
	       const token = socket.handshake.auth.token;

	       try {
	           await axios.post("http://localhost:8000/api/last-seen", {}, {
	               headers: {
	                   Authorization: "Bearer " + token
	               }
	           });
	       } catch (e) {}
	   });
	   socket.on('disconnect',async () => {
	       const userId = socket.userId;
	       if (!userId) return;
		   const now = Date.now();
		   try {
		       await axios.post('http://localhost:8000/api/last-seen', {}, {
		           headers: {
		               Authorization: 'Bearer ' + socket.token // مهم
		           }
		       });
		   } catch (e) {
		       console.error('last_seen update failed', e.message);
		   }
	       const sockets = onlineUsers.get(userId);
	       if (sockets) {
	           sockets.delete(socket.id);
	           if (sockets.size === 0) {
	               onlineUsers.delete(userId);
	               //lastActivity.delete(userId);
				   lastActivity.set(userId, now); 
	           }
	       }
		   gellybookns.emit('user.offline', {
		          userId: userId,
		          lastSeen: now
		      });
	       broadcastOnlineUsers();
	   });
   });

server.listen(3000, () =>
    console.log('Socket.IO running on 3000')
);
