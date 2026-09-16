import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import axios from 'axios';
import cors from 'cors';

const app = express();
const port = process.env.PORT || 8080;

// ====== CORS ======
app.use(cors({
    origin: ['https://gellysocial.vercel.app', 'http://localhost:8000', 'http://localhost:3000', '*'],
    methods: ["GET", "POST"],
    credentials: true
}));
app.use(express.json());

app.get("/", (req, res) => {
    res.send("Socket.IO Server running on port: " + port);
});

const server = createServer(app);
const io = new Server(server, {
    cors: {
        origin: process.env.NODE_ENV === 'production' 
            ? ['https://gellysocial.vercel.app', 'https://gellysocial.vercel.app', '*']
            : '*',
        methods: ["GET", "POST"],
        credentials: true
    },
    path: "/gellybook/",
    pingInterval: 5000,
    pingTimeout: 10000,
    transports: ['websocket']
});

// ====== المتغيرات ======
const lastSeen = new Map();
const onlineUsers = new Map();
const lastActivity = new Map();
const userTokens = new Map();
const lastApiUpdate = new Map();
const OFFLINE_TIMEOUT = 30000;
const ONLINE_BROADCAST_INTERVAL = 15000;
let openChats = {};

const gellybookns = io.of("/gellybook");

// ====== دوال مساعدة ======
function setOnline(userId, socketId) {
    if (!onlineUsers.has(userId)) {
        onlineUsers.set(userId, new Set());
    }
    onlineUsers.get(userId).add(socketId);
}

function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', { users: onlineList });
}

// ====== تحديث حالة المستخدمين ======
setInterval(async () => {
    const now = Date.now();

    for (const [userId, lastAct] of lastActivity.entries()) {
        const isOnline = onlineUsers.has(userId);
        const timeSinceLastActivity = now - lastAct;

        if (isOnline && timeSinceLastActivity > OFFLINE_TIMEOUT) {
            onlineUsers.delete(userId);
            lastSeen.set(userId, now);
            userTokens.delete(userId);
            gellybookns.emit('user.offline', { userId, lastSeen: now });
        }
    }
    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

// ====================================================
// اتصال Socket.IO
// ====================================================
gellybookns.on('connection', socket => {
    const token = socket.handshake.auth.token;
    const userId = socket.handshake.auth.userId;
    
    lastSeen.delete(userId);

    if (userId && token) {
        userTokens.set(userId, token);
        if (!lastActivity.has(userId)) lastActivity.set(userId, Date.now());
    }
    socket.userId = userId;
    socket.token = token;

    // ====== الانضمام للغرف ======
    if (userId) {
        socket.join('chat.' + userId);
        socket.join('user.' + userId);
        setOnline(userId, socket.id);
        gellybookns.emit('user.online', { userId });
        broadcastOnlineUsers();
    }

    // ====================================================
    // أحداث WebRTC (المكالمات)
    // ====================================================
    
    // 1. إرسال عرض مكالمة (Offer)
  // ====== أحداث WebRTC ======
    socket.on('call.offer', ({ to, offer, type, callerName }) => {
        console.log(`📞 Call offer from ${userId} to ${to}`);
        gellybookns.to('chat.' + to).emit('call.offer', {
            from: userId,
            offer: offer,
            type: type,
            callerName: callerName || 'مستخدم'
        });
    });

    socket.on('call.answer', ({ to, answer }) => {
        console.log(`📞 Call answer from ${userId} to ${to}`);
        gellybookns.to('chat.' + to).emit('call.answer', {
            from: userId,
            answer: answer
        });
    });

    socket.on('call.ice', ({ to, candidate }) => {
        gellybookns.to('chat.' + to).emit('call.ice', {
            from: userId,
            candidate: candidate
        });
    });

    socket.on('call.accept', ({ toUserId }) => {
        gellybookns.to('chat.' + toUserId).emit('call.answered');
    });

    socket.on('call.reject', ({ toUserId }) => {
        gellybookns.to('chat.' + toUserId).emit('call.rejected');
    });

    socket.on('call-ended', ({ to, from, duration }) => {
        console.log(`📞 Call ended: ${from} -> ${to}, duration: ${duration}s`);
        gellybookns.to('chat.' + to).emit('call-ended', { from, duration });
    });

    socket.on('call_missed', ({ to, from, chatId }) => {
        gellybookns.to('chat.' + to).emit('call_missed', { from, chatId });
    });

    // ====== أحداث الشات ======
    socket.on('join', (room) => {
        socket.join(room);
        console.log(`📌 ${userId} joined room: ${room}`);
        const userIdFromRoom = String(room).split('.').pop();
        setOnline(userIdFromRoom, socket.id);
        if (!openChats[userIdFromRoom]) openChats[userIdFromRoom] = [];
        lastActivity.set(userIdFromRoom, Date.now());
        if (userIdFromRoom && token) {
            userTokens.set(userIdFromRoom, token);
        }
        socket.userId = userIdFromRoom;
        broadcastOnlineUsers();
    });

    socket.on('join-profile', userId => {
        socket.join(`profile.${userId}`);
    });

    socket.on('heartbeat', () => {
        const uid = socket.userId;
        if (!uid) return;
        lastActivity.set(uid, Date.now());
        if (onlineUsers.has(uid)) {
            onlineUsers.get(uid).add(socket.id);
        } else {
            onlineUsers.set(uid, new Set([socket.id]));
            gellybookns.emit('user.online', { userId: uid });
            broadcastOnlineUsers();
        }
        lastActivity.set(uid, Date.now());
    });

    socket.on("useristyping", ({ sender, receiver }) => {
        lastActivity.set(socket.userId, Date.now());
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
        const last = lastSeen.get(uid);
        return callback({
            lastSeen: last || lastActivity.get(uid) || null
        });
    });

    socket.on('chat.opened', ({ chatWith }) => {
        const uid = socket.userId;
        if (!openChats[uid]) openChats[uid] = [];
        if (!openChats[uid].includes(chatWith)) {
            openChats[uid].push(chatWith);
        }
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
        const uid = socket.userId;
        if (!uid) return;
        const logoutTime = Date.now();
        const sockets = onlineUsers.get(uid);
        if (sockets) {
            sockets.forEach(sid => {
                const s = gellybookns.sockets.get(sid);
                if (s) s.disconnect(true);
            });
        }
        onlineUsers.delete(uid);
        lastSeen.set(uid, logoutTime);
        lastActivity.set(uid, logoutTime);
        gellybookns.emit('user.offline', { userId: uid, lastSeen: logoutTime });
        broadcastOnlineUsers();
    });

    // ====== Webhook ======
    app.post('/webhook', express.json(), (req, res) => {
        const { event, data } = req.body;
        if (!event || !data) {
            return res.status(400).send('Missing event or data');
        }

        if (event === 'message.sent' || event === 'message.deleted') {
            const senderRoom = 'chat.' + data.sender_id;
            const receivearRoom = 'chat.' + data.receiver_id;
            gellybookns.to(senderRoom).emit(event, data);
            gellybookns.to(receivearRoom).emit(event, data);
            console.log("Message sent or deleted");
        } 
        else if (event === 'message.sent.group' || event === 'message.deleted.group') {
            gellybookns.to('groups.' + data.data.group_id).emit(event, data.data);
            console.log(data.data.group_id);
        } 
        else if (event === 'post.newpost') {
            gellybookns.to('newpost.' + data.receiver_id).emit(event, data);
        } 
        else if (event === 'message.friendrequestsent') {
            gellybookns.to('friendrequestsent.' + data.receiver_id).emit(event, data);
        } 
        else if (event === 'message.friendrequestcanceled') {
            gellybookns.to('friendrequestcanceled.' + data.receiver_id).emit(event, data);
        }
        else if (event === 'follow.page') {
            gellybookns.to('page.' + data.receiver_id).emit(event, data);
        } 
        else if (event === 'follow.group') { 
            console.log("FOllow group : " + data.receiver_id);
            gellybookns.to('group.' + data.receiver_id).emit(event, data);
        }

        res.status(200).send('Event processed');
    });

    // ====== قطع الاتصال ======
    socket.on('disconnect', () => {
        const uid = socket.userId;
        if (!uid) return;
        const sockets = onlineUsers.get(uid);
        if (!sockets) return;
        sockets.delete(socket.id);
        if (sockets.size === 0) {
            onlineUsers.delete(uid);
            const now = Date.now();
            lastSeen.set(uid, now);
            lastActivity.set(uid, now);
            gellybookns.emit('user.offline', { userId: uid, lastSeen: now });
        }
        broadcastOnlineUsers();
    });
});

server.listen(port, () => {
    console.log(`✅ Socket.IO running on port: ${port}`);
});
