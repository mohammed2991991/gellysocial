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

const onlineUsers = new Map();        // userId -> Set of socket.id
const lastActivity = new Map();       // userId -> last activity timestamp
const userTokens = new Map();         // userId -> token (for API calls)
const lastApiUpdate = new Map();      // userId -> last time we called /api/last-seen

const ONLINE_BROADCAST_INTERVAL = 90000;
const LAST_SEEN_UPDATE_INTERVAL = 30000; // 30 seconds

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


// تحديث تلقائي لـ lastSeen في قاعدة البيانات كل 30 ثانية
/*setInterval(async () => {
    const now = Date.now();
    for (const [userId, lastAct] of lastActivity.entries()) {
        const lastUpdate = lastApiUpdate.get(userId) || 0;
        // إذا كان هناك نشاط جديد بعد آخر تحديث، أو مر أكثر من 30 ثانية على آخر تحديث
        if (lastAct > lastUpdate || (now - lastUpdate) >= LAST_SEEN_UPDATE_INTERVAL) {
            const token = userTokens.get(userId);
            if (!token) continue; // لا يمكن تحديث بدون توكن
            
            try {
                await axios.post('http://localhost:8000/api/last-seen', {}, {
                    headers: { Authorization: 'Bearer ' + token }
                });
                lastApiUpdate.set(userId, now);
              //  console.log(`Updated lastSeen for user ${userId}`);
            } catch (err) {
                console.error(`Failed to update lastSeen for user ${userId}:`, err.message);
            }
        }
		gellybookns.emit('user.lastSeen.update', {
		    userId,
		    lastSeen: lastAct
		});
		const onlineList = Array.from(onlineUsers.keys());
		  gellybookns.emit('friends.online.list', {
		      users: onlineList
		  });
    }
}, LAST_SEEN_UPDATE_INTERVAL);
*/
// Socket.IO connection handler

setInterval(async () => {
    const now = Date.now();
    const toDelete = []; // لتجميع userIds التي يجب حذفها

    for (const [userId, lastAct] of lastActivity.entries()) {
        const lastUpdate = lastApiUpdate.get(userId) || 0;
        const token = userTokens.get(userId);

        // إذا لم يوجد توكن، لا يمكن تحديث lastSeen -> حذف المدخل
        if (!token  || !onlineUsers.has(userId)) {
            
			lastActivity.delete(userId);
			userTokens.delete(userId);
            continue;
        }

        if (lastAct > lastUpdate || (now - lastUpdate) >= LAST_SEEN_UPDATE_INTERVAL) {
            try {
                await axios.post('http://localhost:8000/api/last-seen', {}, {
                    headers: { Authorization: 'Bearer ' + token }
                });
                lastApiUpdate.set(userId, now);
            } catch (err) {
                const status = err.response?.status;
                console.error(`Failed to update lastSeen for user ${userId}: status=${status}`);
                if (status === 401) {

					userTokens.delete(userId);
					      lastActivity.delete(userId);
					      onlineUsers.delete(userId);
					}
            }
        }

        // إرسال التحديث للعميل (حتى للمستخدمين الذين سيتم حذفهم لاحقًا)
        gellybookns.emit('user.lastSeen.update', { userId, lastSeen: lastAct });
    }

    // حذف المستخدمين الذين فشلوا أو ليس لديهم توكن
    for (const userId of toDelete) {
        lastActivity.delete(userId);
        lastApiUpdate.delete(userId);
        console.log(`Cleaned up lastActivity for user ${userId} due to missing/invalid token`);
    }

    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

/*setInterval(() => {
    const now = Date.now();
    const updates = [];

    for (const [userId, lastAct] of lastActivity.entries()) {

        // 👇 حماية من القيم القديمة جدًا
        const safeLastSeen = Math.max(lastAct, now - 1000 * 60 * 60 * 24 * 7);

        updates.push({
            userId,
            lastSeen: safeLastSeen
        });
    
		}

    gellybookns.emit('users.lastSeen.bulk', updates);
    broadcastOnlineUsers();

}, 10000);
*/
gellybookns.on('connection', socket => {
    const token = socket.handshake.auth.token;
    const userId = socket.handshake.auth.userId;
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

	    lastActivity.set(userId, Date.now());

	    // لو مش متسجل online ضيفه
	    if (!onlineUsers.has(userId)) {
	        onlineUsers.set(userId, new Set());
	        onlineUsers.get(userId).add(socket.id);

	        gellybookns.emit('user.online', { userId }); // 🔥 بث online
	    }
	});
    socket.on('join', (room) => {
        socket.join(room);
        const userIdFromRoom = String(room).split('.').pop();
        if (!openChats[userIdFromRoom]) openChats[userIdFromRoom] = [];
        
        // تحديث آخر نشاط وتخزين التوكن
        lastActivity.set(userIdFromRoom, Date.now());
        if (userIdFromRoom && token && !userTokens.has(userIdFromRoom)) {
            userTokens.set(userIdFromRoom, token);
        }

        socket.userId = userIdFromRoom;

        if (!onlineUsers.has(userIdFromRoom)) {
            onlineUsers.set(userIdFromRoom, new Set());
        }
        onlineUsers.get(userIdFromRoom).add(socket.id);
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

    socket.on('get-last-seen', async (userId, callback) => {
        try {
            const sockets = onlineUsers.get(String(userId));
            if (sockets && sockets.size > 0) {
                callback({ lastSeen: 'online' });
            } else {
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
	socket.on('member.logout', () => {
	    const userId = socket.userId;
	    if (!userId) return;

	    // 🧹 تنظيف كامل
	    userTokens.delete(userId);
	    lastActivity.delete(userId);
	    lastApiUpdate.delete(userId);
	    onlineUsers.delete(userId);

	    socket.disconnect(true);

	    console.log(`User ${userId} logged out clean`);
	});
	/*	socket.on('member.logout', async ({ userId }) => {
	    if (!userId) return;
	    console.log(`Member logout requested for user ${userId}`);
	    
	    const token = userTokens.get(userId);
	    if (token) {
	        try {
	            await axios.post('http://localhost:8000/api/last-seen', {}, {
	                headers: { Authorization: 'Bearer ' + token }
	            });
	            lastApiUpdate.set(userId, Date.now());
	            console.log(`Last seen updated for user ${userId} (on logout)`);
	        } catch (err) {
	            console.error(`Failed to update lastSeen for user ${userId} on logout:`, err.message);
	            // إذا كان 401 يتم حذف التوكن لاحقًا على أي حال، لكننا سنحذفه دائمًا
	        }
	    }
	    
	    // ✅ إزالة التوكن نهائيًا بعد محاولة التحديث (سواء نجحت أم لا)
	    userTokens.delete(userId);
	    lastApiUpdate.delete(userId); // اختياري: تنظيف خريطة آخر تحديث
	    
	    // إزالة المستخدم من قائمة المتصلين
	    const sockets = onlineUsers.get(userId);
	    if (sockets) {
	        for (const sid of sockets) {
	            const targetSocket = gellybookns.sockets.get(sid);
	            if (targetSocket && targetSocket.id !== socket.id) {
	                targetSocket.disconnect(true);
	            }
	        }
	        onlineUsers.delete(userId);
	    }
	    
	    // تحديث آخر نشاط (سيُستخدم لعرض "آخر ظهور")
	    lastActivity.set(userId, Date.now());
	    
	    // إعلام الجميع بأن المستخدم غير متصل
	    gellybookns.emit('user.offline', { userId, lastSeen: Date.now() });
	    broadcastOnlineUsers();
	});

		  socket.on('heartbeat', ({ token }) => {
		if (socket.userId) {
		       lastActivity.set(socket.userId, Date.now());
		       if (token) {
		           userTokens.set(socket.userId, token);
		       }
		   }
    });*/

/*    socket.on('updateLastSeen', async () => {
        // يمكن الاحتفاظ بها كطريقة يدوية أيضاً
        const token = socket.handshake.auth.token;
        try {
            await axios.post("http://localhost:8000/api/last-seen", {}, {
                headers: { Authorization: "Bearer " + token }
            });
            if (socket.userId) lastApiUpdate.set(socket.userId, Date.now());
        } catch (e) {}
    });*/

	socket.on('disconnect', () => {
	    const userId = socket.userId;
	    if (!userId) return;

	    lastActivity.set(userId, Date.now());
	    onlineUsers.delete(userId);

	    broadcastOnlineUsers();
	    console.log("Socket disconnected cleanly");
	});
});

server.listen(3000, () =>
    console.log('Socket.IO running on 3000')
);