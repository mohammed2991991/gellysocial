import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';
import axios from 'axios';

const app = express();
const server = createServer(app);
const io = new Server(server, { 
	cors: { 
		origin: '*',	
		transports: ['websocket', 'polling']  
	},	
	path: "/gellybook/",
	pingInterval: 5000,
	pingTimeout: 10000
});
function setOnline(userId, socketId) {
    if (!onlineUsers.has(userId)) {
        onlineUsers.set(userId, new Set());
    }

    onlineUsers.get(userId).add(socketId);
}
function touch(userId) {
    lastActivity.set(userId, Date.now());
}
const lastSeen = new Map();
const onlineUsers = new Map();        // userId -> Set of socket.id
const lastActivity = new Map();       // userId -> last activity timestamp
const userTokens = new Map();         // userId -> token (for API calls)
const lastApiUpdate = new Map();      // userId -> last time we called /api/last-seen

const ONLINE_BROADCAST_INTERVAL = 15000;
const LAST_SEEN_UPDATE_INTERVAL = 60000; 

const redis = new Redis();
let openChats = {};
redis.psubscribe('*');

const gellybookns = io.of("/gellybook");

redis.on('pmessage', (pattern, channel, message) => {
    const payload = JSON.parse(message);
    if (!payload?.data) return;
    const event = payload.event;
    const data = payload.data;
    if (event === 'message.sent' || event === 'message.deleted') {
        const senderRoom = 'chat.' + payload.data.sender_id;
        const receiverRoom = 'chat.' + payload.data.receiver_id;
        gellybookns.to(senderRoom).emit(event, payload.data);
        gellybookns.to(receiverRoom).emit(event, payload.data);
    } else if (event === 'message.seen' || event === 'message.delivered') {
        const msg = data.message;
        const messageId = data.message_id;
        const status = data.status;
        if (!msg || !messageId || !status) return;
        const senderRoom = 'chat.' + msg.member_id;
        const receiverRoom = 'chat.' + (data.receiver_id || msg.member_id);
        gellybookns.to(senderRoom).emit(event, data);
        gellybookns.to(receiverRoom).emit(event, data);
    } else if (event === 'message.sent.group' || event === 'message.deleted.group') {
        const groupRoom = 'group.' + payload.data.group_id;
        gellybookns.to(groupRoom).emit(event, payload.data);
    } else if (event === 'post.newpost') {
        const receiverRoom = 'newpost.' + payload.data.receiver_id;
        gellybookns.to(receiverRoom).emit(event, payload.data);
    } else if (event === 'message.friendrequestsent') {
        const receiverRoom = 'friendrequestsent.' + payload.data.receiver_id;
        gellybookns.to(receiverRoom).emit(event, payload.data);
    } else if (event === 'message.friendrequestcanceled') {
        const receiverRoom = 'friendrequestcanceled.' + payload.data.receiver_id;
        gellybookns.to(receiverRoom).emit(event, payload.data);
    }
});

// دالة بث المستخدمين المتصلين
function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', { users: onlineList });
}


setInterval(async () => {
    const now = Date.now();
    const toDelete = []; // لتجميع userIds التي يجب حذفها

    for (const [userId, lastAct] of lastActivity.entries()) {
        const lastUpdate = lastApiUpdate.get(userId) || 0;
        const token = userTokens.get(userId);
		if (!onlineUsers.has(userId)) {
		 //   lastSeen.set(userId, lastAct);
		}
		if (onlineUsers.has(userId)) continue;
		gellybookns.emit('user.lastSeen.update', {
		    userId,
		    lastSeen: lastSeen.get(userId) || lastActivity.get(userId)
		});
	
		if (!token) {
		    userTokens.delete(userId);
		    continue;
		}
        if (lastAct > lastUpdate || (now - lastUpdate) >= LAST_SEEN_UPDATE_INTERVAL) {
            try {
             /*   await axios.post('http://localhost:8000/api/last-seen', {}, {
                    headers: { Authorization: 'Bearer ' + token }
                });*/
                lastApiUpdate.set(userId, now);
            } catch (err) {
                const status = err.response?.status;
                console.error(`Failed to update lastSeen for user ${userId}: status=${status}`);
                if (status === 401) {

					userTokens.delete(userId);
					      lastActivity.delete(userId);
						  lastSeen.delete(userId);
					      onlineUsers.delete(userId);
					}
            }
        }

    }

    // حذف المستخدمين الذين فشلوا أو ليس لديهم توكن
    for (const userId of toDelete) {
        lastActivity.delete(userId);
        lastApiUpdate.delete(userId);
        console.log(`Cleaned up lastActivity for user ${userId} due to missing/invalid token`);
    }

    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);


gellybookns.on('connection', socket => {
    const token = socket.handshake.auth.token;
    const userId = socket.handshake.auth.userId;
	lastSeen.delete(userId);

	if (userId && token) {
	     userTokens.set(userId, token);  // ✅ دائماً استبدل التوكن القديم بالجديد
	     if (!lastActivity.has(userId)) lastActivity.set(userId, Date.now());
	 }
    socket.userId = userId;
    socket.token = token;

    // تخزين التوكن إذا كان جديداً
	if (userId && token) {
	    userTokens.set(userId, token); // دايمًا update
	}

	
	// في خادم Socket.IO (gellybookns)
	socket.on('call.start', ({ toUserId, callerName, callType }) => {
	    const targetRoom = 'chat.' + toUserId;
	    socket.to(targetRoom).emit('call.incoming', {
	        fromUserId: socket.userId,
	        callerName: callerName,
	        callType: callType
	    });
	});

	socket.on('call.accept', ({ toUserId }) => {
	    const targetRoom = 'chat.' + toUserId;
	    socket.to(targetRoom).emit('call.answered');
	});

	socket.on('call.reject', ({ toUserId }) => {
	    const targetRoom = 'chat.' + toUserId;
	    socket.to(targetRoom).emit('call.rejected');
	});
	
	
	
    socket.on('join-profile', userId => {
        socket.join(`profile.${userId}`);
    });
	socket.on('heartbeat', () => {
	    const userId = socket.userId;
	    if (!userId) return;

	    // تحديث آخر نشاط
	    lastActivity.set(userId, Date.now());

	    // التأكد من أن socket.id موجود في المجموعة (في حال فُقد لأي سبب)
	    if (onlineUsers.has(userId)) {
			
	        onlineUsers.get(userId).add(socket.id);
	    } else {
	        // حالة نادرة: يعيد إنشاء المجموعة إذا اختفت لأي سبب
	        onlineUsers.set(userId, new Set([socket.id]));
	        gellybookns.emit('user.online', { userId });
	        broadcastOnlineUsers();
	      //  lastSeen.delete(userId);
	    }
		lastActivity.set(userId, Date.now());
	});

		socket.on('join', (room) => {
	    socket.join(room);
	    const userIdFromRoom = String(room).split('.').pop();

	    setOnline(userIdFromRoom, socket.id);  // تضمن وجوده في onlineUsers

	    if (!openChats[userIdFromRoom]) openChats[userIdFromRoom] = [];

	    lastActivity.set(userIdFromRoom, Date.now());

	    if (userIdFromRoom && token) {
	        userTokens.set(userIdFromRoom, token);
	    }

	    socket.userId = userIdFromRoom;

	    // لا حاجة broadcastOnlineUsers() هنا لأنها قد تتكرر، لكن يمكن الاحتفاظ بها إذا أردت
	    broadcastOnlineUsers();
	});
  
	
	  socket.on("useristyping", ({ sender, receiver }) => {
        lastActivity.set(socket.userId, Date.now());
        console.log("typing : " + sender + " + " + receiver);
        const senderRoom = 'chat.' + sender;
        const receiverRoom = 'chat.' + receiver;
        gellybookns.to(senderRoom).emit('useristyping', { sender, receiver });
        gellybookns.to(receiverRoom).emit('useristyping', { sender, receiver });
    });

	socket.on('get-last-seen', (userId, callback) => {
	    const uid = String(userId);
	    if (onlineUsers.has(uid)) {
	        return callback({ lastSeen: 'online' });
	    }
	   /* const last = lastSeen.get(uid) || lastActivity.get(uid) || null;
	    callback({ lastSeen: last });*/
		if (onlineUsers.has(uid)) {
		    return callback({ lastSeen: 'online' });
		}

		const last = lastSeen.get(uid);

		return callback({
		    lastSeen: last || lastActivity.get(uid) || null
		});
	});
 

   socket.on('chat.opened', ({ chatWith }) => {
        const userId = socket.userId;
        if (!openChats[userId]) openChats[userId] = [];
        if (!openChats[userId].includes(chatWith)) {
            openChats[userId].push(chatWith);
        }
        // باقي منطق chat.opened حسب قاعدة البيانات الخاصة بك
    });

    socket.on('chat.closed', ({ chatWith }) => {
        if (openChats[socket.userId]) {
            openChats[socket.userId] = openChats[socket.userId].filter(id => id !== chatWith);
        }
    });
	socket.on('user.offline', () => {
	    if (socket.userId) {
	        userTokens.delete(socket.userId);
	        lastActivity.delete(socket.userId);
	    }
	});
	socket.on('member.logout', async () => {
	    const userId = socket.userId;
	    if (!userId) return;

	    const logoutTime = Date.now();

	    // 👇 اقفل كل sockets لنفس اليوزر
	    const sockets = onlineUsers.get(userId);
	    if (sockets) {
	        sockets.forEach(sid => {
	            const s = gellybookns.sockets.get(sid);
	            if (s) s.disconnect(true);
	        });
	    }

	    // 👇 احذفه من online
	    onlineUsers.delete(userId);

	    // 👇 حدّث lastSeen
	    lastSeen.set(userId, logoutTime);
	    lastActivity.set(userId, logoutTime);

	    // 👇 بلغ الكل
	    gellybookns.emit('user.offline', {
	        userId,
	        lastSeen: logoutTime
	    });

	    broadcastOnlineUsers();
	});

	socket.on('disconnect', () => {
	    const userId = socket.userId;
	    if (!userId) return;

	    const sockets = onlineUsers.get(userId);
	    if (!sockets) return;

	    sockets.delete(socket.id);

	    if (sockets.size === 0) {
	        onlineUsers.delete(userId);

	        const now = Date.now();
	        lastSeen.set(userId, now);
	        lastActivity.set(userId, now);

	        gellybookns.emit('user.offline', {
	            userId,
	            lastSeen: now
	        });
	    }
	});
/*socket.on('disconnect', () => {
    const userId = socket.userId;
    if (!userId) return;

    const sockets = onlineUsers.get(userId);
    if (!sockets) return;

    sockets.delete(socket.id);

    if (sockets.size === 0) {
        onlineUsers.delete(userId);
        const now = Date.now();
        lastSeen.set(userId, now);
        lastActivity.set(userId, now);
        gellybookns.emit('user.offline', { userId, lastSeen: now });
    }

    broadcastOnlineUsers();
});
*/

});

server.listen(3000, () =>
    console.log('Socket.IO running on 3000')
);