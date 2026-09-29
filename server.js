// ═══════════════════════════════════════════════════════════════
// Gellybook Socket.IO Server — نسخة كاملة
// ═══════════════════════════════════════════════════════════════
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import axios from 'axios';
import cors from 'cors';

const app = express();
const port = process.env.PORT || 8080;

// ═══════════════════════════════════════════════════════════════
// CORS + Body Parsers
// ═══════════════════════════════════════════════════════════════
app.use(cors({
    origin: process.env.NODE_ENV === 'production'
            ? ['https://gellysocial.vercel.app', 'https://gellysocial.vercel.app', '*']
            : '*',
    methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
    credentials: true,
}));
app.use(express.json({limit: '1mb'}));
app.use(express.urlencoded({extended: true}));

// ═══════════════════════════════════════════════════════════════
// HTTP Server + Socket.IO
// ═══════════════════════════════════════════════════════════════
const server = createServer(app);

const io = new Server(server, {
    cors: {
        origin: process.env.NODE_ENV === 'production'
                ? ['https://gellysocial.vercel.app', '*']
                : '*',
        methods: ['GET', 'POST'],
        credentials: true,
    },
    path: '/gellybook/',
    pingInterval: 30000, // ✅ 25 → 30 ثانية
    pingTimeout: 90000, // ✅ 60 → 90 ثانية
    transports: ['websocket', 'polling'],
});

const gellybookns = io.of('/gellybook');

// ═══════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════
const lastSeen = new Map();
const onlineUsers = new Map();
const lastActivity = new Map();
const userTokens = new Map();
const lastApiUpdate = new Map();

const OFFLINE_TIMEOUT = 120000;  // ✅ 30 → 120 ثانية
const ONLINE_BROADCAST_INTERVAL = 20000;   // ✅ 15 → 20 ثانية

let openChats = {};

// ═══════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════
function setOnline(userId, socketId) {
    if (!onlineUsers.has(userId)) {
        onlineUsers.set(userId, new Set());
    }
    onlineUsers.get(userId).add(socketId);
}

function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', {users: onlineList});
}

function safeEmit(room, event, payload) {
    try {
        gellybookns.to(room).emit(event, payload);
        console.log(`→ emit "${event}" to "${room}"`);
    } catch (e) {
        console.error(`❌ emit failed (${event} → ${room}):`, e.message);
    }
}

// ═══════════════════════════════════════════════════════════════
// Health
// ═══════════════════════════════════════════════════════════════
app.get('/', (req, res) => {
    res.send('Socket.IO Server running on port: ' + port);
});

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        online: onlineUsers.size,
        uptime: process.uptime(),
        timestamp: Date.now(),
    });
});

// ═══════════════════════════════════════════════════════════════
// ✅ WEBHOOK — على المستوى العلوي (مش جوه connection)
// ═══════════════════════════════════════════════════════════════
app.post('/webhook', (req, res) => {
    const {event, data} = req.body;

    console.log(`\n📥 [webhook] event="${event}"`);
    console.log('   data:', JSON.stringify(data, null, 2));

    if (!event || !data) {
        console.warn('⚠️ Missing event or data');
        return res.status(400).json({error: 'Missing event or data'});
    }

    try {
        switch (event) {
            // ═══ Chat (1-to-1) ═══
            case 'message.sent':
            case 'message.deleted':
                safeEmit('chat.' + data.sender_id, event, data);
                safeEmit('chat.' + data.receiver_id, event, data);
                break;

                // ═══ Chat (Group) ═══
            case 'message.sent.group':
            case 'message.deleted.group':
                safeEmit('groups.' + data.data.group_id, event, data.data);
                break;

                // ═══ Friend Requests ═══
            case 'message.friendrequestsent':
                safeEmit('friendrequestsent.' + data.receiver_id, event, data);
                break;

            case 'message.friendrequestcanceled':
                safeEmit('friendrequestcanceled.' + data.receiver_id, event, data);
                break;

                // ═══ Posts ═══
            case 'post.newpost':
                safeEmit('newpost.' + data.receiver_id, event, data);
                break;

                // ═══ ✅ Comments (new) ═══
            case 'comment.new':
                safeEmit('newpost.' + data.receiver_id, event, data);
                break;

                // ═══ ✅ Comment Reactions ═══
            case 'comment.reaction':
                safeEmit('newpost.' + data.receiver_id, event, data);
                break;

                // ═══ Follow ═══
            case 'follow.page':
                safeEmit('page.' + data.receiver_id, event, data);
                break;

            case 'follow.group':
                safeEmit('group.' + data.receiver_id, event, data);
                break;
                // ═══════════════════════════════════════════════════════════════
// داخل switch (event) — ضيف الحالات دي مع الباقي
// ═══════════════════════════════════════════════════════════════

// ✅ Story Comment
            case 'story.comment':
                safeEmit('newpost.' + data.receiver_id, event, data);
                break;

// ✅ Story Reaction
            case 'story.reaction':
                safeEmit('newpost.' + data.receiver_id, event, data);
                break;

            default:
                console.warn(`⚠️ Unknown event: ${event}`);
        }

        res.status(200).json({status: 'ok', event});
    } catch (err) {
        console.error('❌ webhook error:', err);
        res.status(500).json({error: err.message});
    }
});

// ═══════════════════════════════════════════════════════════════
// Periodic cleanup
// ═══════════════════════════════════════════════════════════════
setInterval(() => {
    const now = Date.now();

    for (const [userId, lastAct] of lastActivity.entries()) {
        const isOnline = onlineUsers.has(userId);
        const idle = now - lastAct;

        if (isOnline && idle > OFFLINE_TIMEOUT) {
            onlineUsers.delete(userId);
            lastSeen.set(userId, now);
            userTokens.delete(userId);
            gellybookns.emit('user.offline', {userId, lastSeen: now});
            console.log(`💤 user ${userId} timed out → offline (idle=${Math.floor(idle / 1000)}s)`);
        }
    }
    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

// ═══════════════════════════════════════════════════════════════
// SOCKET CONNECTION
// ═══════════════════════════════════════════════════════════════
gellybookns.on('connection', (socket) => {
    const token = socket.handshake.auth?.token;
    const userId = socket.handshake.auth?.userId;

    // ✅ خزّن الـ userId الحقيقي — مايتغيرش أبداً
    socket.userId = userId;
    socket.realUserId = userId;
    socket.token = token;

    console.log(`\n🔌 Connected: user=${userId} socket=${socket.id}`);

    lastSeen.delete(userId);

    if (userId && token) {
        userTokens.set(userId, token);
        if (!lastActivity.has(userId))
            lastActivity.set(userId, Date.now());
    }

    // ═══ الانضمام التلقائي ═══
    if (userId) {
        socket.join('chat.' + userId);
        socket.join('user.' + userId);
        setOnline(userId, socket.id);
        gellybookns.emit('user.online', {userId});
        broadcastOnlineUsers();
    }

    // ═══════════════════════════════════════════════════════════
    // WebRTC
    // ═══════════════════════════════════════════════════════════
    socket.on('call.offer', ({ to, offer, type, callerName }) => {
        console.log(`📞 call.offer from ${socket.realUserId} to ${to}`);
        safeEmit('chat.' + to, 'call.offer', {
            from: socket.realUserId,
            offer,
            type,
            callerName: callerName || 'مستخدم',
        });
    });

    socket.on('call.answer', ({ to, answer }) => {
        safeEmit('chat.' + to, 'call.answer', {
            from: socket.realUserId,
            answer,
        });
    });

    socket.on('call.ice', ({ to, candidate }) => {
        safeEmit('chat.' + to, 'call.ice', {
            from: socket.realUserId,
            candidate,
        });
    });

    socket.on('call.accept', ({ toUserId }) => {
        safeEmit('chat.' + toUserId, 'call.answered', {});
    });

    socket.on('call.reject', ({ toUserId }) => {
        safeEmit('chat.' + toUserId, 'call.rejected', {});
    });

    socket.on('call-ended', ({ to, from, duration }) => {
        console.log(`📞 call-ended: ${from} → ${to} (${duration}s)`);
        safeEmit('chat.' + to, 'call-ended', {from, duration});
    });

    socket.on('call_missed', ({ to, from, chatId }) => {
        safeEmit('chat.' + to, 'call_missed', {from, chatId});
    });

    // ═══════════════════════════════════════════════════════════
    // ✅ join — بدون الكتابة على socket.userId
    // ═══════════════════════════════════════════════════════════
    socket.on('join', (room) => {
        if (typeof room !== 'string' || !room) {
            console.warn('⚠️ invalid room:', room);
            return;
        }
        socket.join(room);
        console.log(`📌 user=${socket.realUserId} joined room: ${room}`);

        // ✅ لا نعدّل socket.userId
        if (socket.realUserId) {
            lastActivity.set(socket.realUserId, Date.now());
        }
    });

    socket.on('join-profile', (uid) => {
        socket.join(`profile.${uid}`);
        console.log(`👤 user=${socket.realUserId} joined profile.${uid}`);
    });

    // ═══ Heartbeat ═══
    socket.on('heartbeat', () => {
        const uid = socket.realUserId;
        if (!uid)
            return;

        lastActivity.set(uid, Date.now());

        if (onlineUsers.has(uid)) {
            onlineUsers.get(uid).add(socket.id);
        } else {
            onlineUsers.set(uid, new Set([socket.id]));
            gellybookns.emit('user.online', {userId: uid});
            broadcastOnlineUsers();
        }
    });

    // ═══ Typing ═══
    socket.on('useristyping', ({ sender, receiver }) => {
        if (socket.realUserId) {
            lastActivity.set(socket.realUserId, Date.now());
        }
        safeEmit('chat.' + sender, 'useristyping', {sender, receiver});
        safeEmit('chat.' + receiver, 'useristyping', {sender, receiver});
    });

    // ═══ Last Seen ═══
    socket.on('get-last-seen', (uid, callback) => {
        const id = String(uid);
        if (onlineUsers.has(id)) {
            return callback({lastSeen: 'online'});
        }
        const last = lastSeen.get(id);
        return callback({
            lastSeen: last || lastActivity.get(id) || null,
        });
    });

    // ═══ Chat opened/closed ═══
    socket.on('chat.opened', ({ chatWith }) => {
        const uid = socket.realUserId;
        if (!uid)
            return;
        if (!openChats[uid])
            openChats[uid] = [];
        if (!openChats[uid].includes(chatWith)) {
            openChats[uid].push(chatWith);
    }
    });

    socket.on('chat.closed', ({ chatWith }) => {
        const uid = socket.realUserId;
        if (!uid || !openChats[uid])
            return;
        openChats[uid] = openChats[uid].filter((id) => id !== chatWith);
    });

    // ═══ Offline يدوي ═══
    socket.on('user.offline', () => {
        const uid = socket.realUserId;
        if (!uid)
            return;
        userTokens.delete(uid);
        lastActivity.delete(uid);
    });

    // ═══ Logout ═══
    socket.on('member.logout', () => {
        const uid = socket.realUserId;
        if (!uid)
            return;

        const logoutTime = Date.now();
        const sockets = onlineUsers.get(uid);

        if (sockets) {
            sockets.forEach((sid) => {
                const s = gellybookns.sockets.get(sid);
                if (s)
                    s.disconnect(true);
            });
        }

        onlineUsers.delete(uid);
        lastSeen.set(uid, logoutTime);
        lastActivity.set(uid, logoutTime);

        gellybookns.emit('user.offline', {userId: uid, lastSeen: logoutTime});
        broadcastOnlineUsers();
        console.log(`🚪 user ${uid} logged out`);
    });

    // ═══ Disconnect ═══
    socket.on('disconnect', (reason) => {
        const uid = socket.realUserId;
        console.log(`🔌 Disconnected: user=${uid} socket=${socket.id} (${reason})`);

        if (!uid)
            return;
        const sockets = onlineUsers.get(uid);
        if (!sockets)
            return;

        sockets.delete(socket.id);

        if (sockets.size === 0) {
            onlineUsers.delete(uid);
            const now = Date.now();
            lastSeen.set(uid, now);
            lastActivity.set(uid, now);
            gellybookns.emit('user.offline', {userId: uid, lastSeen: now});
        }
        broadcastOnlineUsers();
    });
});

// ═══════════════════════════════════════════════════════════════
// Start
// ═══════════════════════════════════════════════════════════════
server.listen(port, () => {
    console.log(`\n✅ Socket.IO running on port: ${port}`);
    console.log(`   Path: /gellybook/`);
    console.log(`   Webhook: POST http://localhost:${port}/webhook\n`);
});

// ═══════════════════════════════════════════════════════════════
// Graceful shutdown
// ═══════════════════════════════════════════════════════════════
process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received, closing...');
    io.close(() => {
        server.close(() => process.exit(0));
    });
});

process.on('uncaughtException', (err) => {
    console.error('❌ uncaughtException:', err);
});

process.on('unhandledRejection', (reason) => {
    console.error('❌ unhandledRejection:', reason);
});
