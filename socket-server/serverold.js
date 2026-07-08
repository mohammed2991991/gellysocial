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

const ONLINE_BROADCAST_INTERVAL = 10000;
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

// تحديث دوري لقائمة المتصلين
setInterval(() => {
    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

// تحديث تلقائي لـ lastSeen في قاعدة البيانات كل 30 ثانية
setInterval(async () => {
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
                console.log(`Updated lastSeen for user ${userId}`);
            } catch (err) {
                console.error(`Failed to update lastSeen for user ${userId}:`, err.message);
            }
        }
		gellybookns.emit('user.lastSeen.update', {
		    userId,
		    lastSeen: lastAct
		});
    }
}, LAST_SEEN_UPDATE_INTERVAL);

// Socket.IO connection handler
gellybookns.on('connection', socket => {
    const token = socket.handshake.auth.token;
    const userId = socket.handshake.auth.userId;

    socket.userId = userId;
    socket.token = token;

    // تخزين التوكن إذا كان جديداً
    if (userId && token && !userTokens.has(userId)) {
        userTokens.set(userId, token);
    }

    socket.on('join-profile', userId => {
        socket.join(`profile.${userId}`);
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

    socket.on('heartbeat', () => {
        if (socket.userId) {
            lastActivity.set(socket.userId, Date.now());
        }
    });

    socket.on('updateLastSeen', async () => {
        // يمكن الاحتفاظ بها كطريقة يدوية أيضاً
        const token = socket.handshake.auth.token;
        try {
            await axios.post("http://localhost:8000/api/last-seen", {}, {
                headers: { Authorization: "Bearer " + token }
            });
            if (socket.userId) lastApiUpdate.set(socket.userId, Date.now());
        } catch (e) {}
    });

    socket.on('disconnect', async () => {
        const userId = socket.userId;
        if (!userId) return;
        const now = Date.now();
        // تحديث lastSeen عند الانفصال (احتياطي)
        try {
            await axios.post('http://localhost:8000/api/last-seen', {}, {
                headers: { Authorization: 'Bearer ' + socket.token }
            });
            lastApiUpdate.set(userId, now);
        } catch (e) {
            console.error('last_seen update failed on disconnect', e.message);
        }
        const sockets = onlineUsers.get(userId);
        if (sockets) {
            sockets.delete(socket.id);
            if (sockets.size === 0) {
                onlineUsers.delete(userId);
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