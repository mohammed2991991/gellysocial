// ═══════════════════════════════════════════════════════════════
// Gellybook Socket.IO Server
// ✅ Online/Offline يعتمد على disconnect فقط (بدون idle kick)
// ✅ Heartbeat مع ack — يكشف الـ stale sockets فوراً
// ✅ pingTimeout أقصر — كشف أسرع
// ═══════════════════════════════════════════════════════════════
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import cors from 'cors';

const app = express();
const port = process.env.PORT || 8080;

app.use(cors({
    origin: process.env.NODE_ENV === 'production'
        ? ['https://gellysocial.vercel.app', '*']
        : '*',
    methods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
    credentials: true,
}));
app.use(express.json({ limit: '1mb' }));
app.use(express.urlencoded({ extended: true }));

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
    // ✅ كشف أسرع للـ stale sockets
    pingInterval: 20000,   // 20s (كان 25)
    pingTimeout: 25000,    // 25s (كان 60) — stale detection خلال 45s كحد أقصى
    transports: ['websocket', 'polling'],
});

const gellybookns = io.of('/gellybook');

function toId(v) {
    return v == null ? null : String(v);
}

// ═══════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════
const lastSeen = new Map();
const onlineUsers = new Map();       // userId → Set<socketId>
const lastActivity = new Map();
const userTokens = new Map();
const openChats = new Map();

const ONLINE_BROADCAST_INTERVAL = 30000;  // 30s (كان 60)

// ═══════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════
function setOnline(userId, socketId) {
    const id = toId(userId);
    if (!id) return false;

    const wasOffline = !onlineUsers.has(id);
    if (!onlineUsers.has(id)) onlineUsers.set(id, new Set());
    onlineUsers.get(id).add(socketId);
    return wasOffline;  // true لو كان offline ورجع online
}

function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', { users: onlineList });
}

function safeEmit(room, event, payload) {
    try {
        gellybookns.to(room).emit(event, payload);
    } catch (e) {
        console.error(`❌ emit failed (${event} → ${room}):`, e.message);
    }
}

function markUserOffline(userId, timestamp = Date.now()) {
    const id = toId(userId);
    if (!id) return;
    lastSeen.set(id, timestamp);
    lastActivity.delete(id);
    gellybookns.emit('user.offline', { userId: id, lastSeen: timestamp });
    console.log(`💤 user ${id} → offline @ ${new Date(timestamp).toISOString()}`);
}

function getNotificationRooms(userId) {
    const id = toId(userId);
    return [
        'chat.' + id,
        'user.' + id,
        'newpost.' + id,
        'group.' + id,
        'page.' + id,
        'friendrequestsent.' + id,
        'friendrequestcanceled.' + id,
    ];
}

// ═══════════════════════════════════════════════════════════════
// Health
// ═══════════════════════════════════════════════════════════════
app.get('/', (req, res) => res.send('Socket.IO Server running on port: ' + port));

app.get('/health', (req, res) => {
    res.json({
        status: 'ok',
        online: onlineUsers.size,
        uptime: process.uptime(),
        timestamp: Date.now(),
        memory: process.memoryUsage(),
    });
});

// ═══════════════════════════════════════════════════════════════
// Webhook
// ═══════════════════════════════════════════════════════════════
app.post('/webhook', (req, res) => {
    const { event, data } = req.body;
    console.log(`\n📥 [webhook] event="${event}"`);
    if (!event || !data) return res.status(400).json({ error: 'Missing event or data' });

    try {
        switch (event) {
            case 'message.sent':
            case 'message.deleted':
                safeEmit('chat.' + toId(data.sender_id), event, data);
                safeEmit('chat.' + toId(data.receiver_id), event, data);
                break;

            case 'message.sent.group':
            case 'message.deleted.group':
                safeEmit('groups.' + toId(data.data?.group_id), event, data.data);
                break;

            case 'message.friendrequestsent':
                safeEmit('friendrequestsent.' + toId(data.receiver_id), event, data);
                break;

            case 'message.friendrequestcanceled':
                safeEmit('friendrequestcanceled.' + toId(data.receiver_id), event, data);
                if (data.sender_id) {
                    safeEmit('friendrequestcanceled.' + toId(data.sender_id), event, data);
                }
                break;

            case 'post.newpost':
            case 'comment.new':
            case 'comment.reaction':
            case 'story.comment':
            case 'story.reaction':
                safeEmit('newpost.' + toId(data.receiver_id), event, data);
                break;

            case 'follow.page':
                safeEmit('page.' + toId(data.receiver_id), event, data);
                break;

            case 'follow.group':
                safeEmit('group.' + toId(data.receiver_id), event, data);
                break;

            case 'message.seen':
                if (data.sender_id) {
                    safeEmit('chat.' + toId(data.sender_id), event, data);
                }
                if (data.viewer_id && toId(data.viewer_id) !== toId(data.sender_id)) {
                    safeEmit('chat.' + toId(data.viewer_id), event, data);
                }
                break;

            default:
                console.warn(`⚠️ Unknown event: ${event}`);
        }
        res.status(200).json({ status: 'ok', event });
    } catch (err) {
        console.error('❌ webhook error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ═══════════════════════════════════════════════════════════════
// ✅ Periodic broadcast فقط — بدون idle kick
// ═══════════════════════════════════════════════════════════════
setInterval(() => {
    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

// ═══════════════════════════════════════════════════════════════
// SOCKET CONNECTION
// ═══════════════════════════════════════════════════════════════
gellybookns.on('connection', (socket) => {
    const token = socket.handshake.auth?.token;
    const userId = toId(socket.handshake.auth?.userId);

    socket.userId = userId;
    socket.realUserId = userId;
    socket.token = token;

    console.log(`\n🔌 Connected: user=${userId} socket=${socket.id}`);

    if (userId) {
        if (!onlineUsers.has(userId)) lastSeen.delete(userId);
        if (token) userTokens.set(userId, token);
        lastActivity.set(userId, Date.now());
        getNotificationRooms(userId).forEach((room) => socket.join(room));
        const wasOffline = setOnline(userId, socket.id);

        if (wasOffline) {
            gellybookns.emit('user.online', { userId });
            broadcastOnlineUsers();
        }

        console.log(`📥 user ${userId} joined:`, getNotificationRooms(userId).join(', '));
    }

    // ═══════════════════════════════════════════════════════════
    // message.send — broadcast فوري + ack للمُرسل
    // ═══════════════════════════════════════════════════════════
    socket.on('message.send', ({ to, message, sender } = {}, ack) => {
        const toId_ = toId(to);

        if (!toId_ || !message) {
            if (typeof ack === 'function') ack({ ok: false, error: 'missing_params' });
            return;
        }

        const payload = {
            sender_id: socket.realUserId,
            receiver_id: toId_,
            message: message,
            sender: sender || { name: 'مستخدم', avatar: null }
        };

        // 1️⃣ المستقبِل
        safeEmit('chat.' + toId_, 'message.sent', payload);

        // 2️⃣ المرسل (تابات تانية)
        gellybookns.to('chat.' + socket.realUserId)
            .except(socket.id)
            .emit('message.sent', payload);

        // ✅ ack فوري (لو العميل طلبه)
        if (typeof ack === 'function') {
            ack({ ok: true, client_id: message.client_id, id: message.id });
        }

        console.log(`📨 message.send ${socket.realUserId} → ${toId_} (client_id=${message.client_id || '-'})`);
    });

    // ═══════════════════════════════════════════════════════════
    // WebRTC
    // ═══════════════════════════════════════════════════════════
    socket.on('call.offer', ({ to, offer, type, callerName, isRestart }) => {
        const toId_ = toId(to);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call.offer', {
            from: socket.realUserId,
            offer, type,
            callerName: callerName || 'مستخدم',
            isRestart: !!isRestart,
        });
    });

    socket.on('call.answer', ({ to, answer }) => {
        const toId_ = toId(to);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call.answer', { from: socket.realUserId, answer });
        safeEmit('chat.' + toId_, 'call.answered', { from: socket.realUserId });
    });

    socket.on('call.ice', ({ to, candidate }) => {
        const toId_ = toId(to);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call.ice', { from: socket.realUserId, candidate });
    });

    socket.on('call.accept', ({ toUserId }) => {
        const toId_ = toId(toUserId);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call.answered', { from: socket.realUserId });
    });

    socket.on('call.reject', ({ toUserId }) => {
        const toId_ = toId(toUserId);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call.rejected', { from: socket.realUserId });
    });

    socket.on('call-ended', ({ to, from, duration }) => {
        const toId_ = toId(to);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call-ended', {
            from: from ?? socket.realUserId,
            duration: duration || 0,
        });
    });

    socket.on('call_missed', ({ to, from, chatId }) => {
        const toId_ = toId(to);
        if (!toId_) return;
        safeEmit('chat.' + toId_, 'call_missed', {
            from: from ?? socket.realUserId,
            chatId: chatId || socket.realUserId,
        });
    });

    // ═══════════════════════════════════════════════════════════
    // Join rooms
    // ═══════════════════════════════════════════════════════════
    socket.on('join', (room) => {
        if (typeof room !== 'string' || !room) return;
        socket.join(room);
        if (socket.realUserId) lastActivity.set(socket.realUserId, Date.now());
    });

    socket.on('join-profile', (uid) => {
        const id = toId(uid);
        if (id) socket.join('profile.' + id);
    });

    // ═══════════════════════════════════════════════════════════
    // ✅ Heartbeat — مع ack فوري (يكشف الـ stale sockets)
    // ═══════════════════════════════════════════════════════════
    socket.on('heartbeat', (callback) => {
        const uid = socket.realUserId;

        // ✅ رد فوري على الـ ack (لو موجود)
        if (typeof callback === 'function') {
            callback({ ok: true, serverTime: Date.now() });
        }

        if (!uid) return;

        lastActivity.set(uid, Date.now());

        if (!onlineUsers.has(uid)) {
            onlineUsers.set(uid, new Set([socket.id]));
            gellybookns.emit('user.online', { userId: uid });
            broadcastOnlineUsers();
            console.log(`💚 user ${uid} back online via heartbeat`);
        } else {
            onlineUsers.get(uid).add(socket.id);
        }
    });

    socket.on('ping', (callback) => {
        const uid = socket.realUserId;
        if (uid) lastActivity.set(uid, Date.now());
        if (typeof callback === 'function') {
            callback({ ok: true, serverTime: Date.now() });
        } else {
            socket.emit('pong');
        }
    });

    // ═══════════════════════════════════════════════════════════
    // Typing
    // ═══════════════════════════════════════════════════════════
    socket.on('useristyping', ({ sender, receiver }) => {
        if (socket.realUserId) lastActivity.set(socket.realUserId, Date.now());
        const senderId = toId(sender);
        const receiverId = toId(receiver);
        if (senderId) safeEmit('chat.' + senderId, 'useristyping', { sender, receiver });
        if (receiverId) safeEmit('chat.' + receiverId, 'useristyping', { sender, receiver });
    });

    // ═══════════════════════════════════════════════════════════
    // get-last-seen
    // ═══════════════════════════════════════════════════════════
    socket.on('get-last-seen', (uid, callback) => {
        const id = toId(uid);
        if (!id || typeof callback !== 'function') return;
        if (onlineUsers.has(id)) return callback({ lastSeen: 'online' });
        return callback({ lastSeen: lastSeen.get(id) || null });
    });

    // ═══════════════════════════════════════════════════════════
    // Chat opened/closed
    // ═══════════════════════════════════════════════════════════
    socket.on('chat.opened', ({ chatWith }) => {
        const uid = socket.realUserId;
        const cw = toId(chatWith);
        if (!uid || !cw) return;
        if (!openChats.has(uid)) openChats.set(uid, new Set());
        openChats.get(uid).add(cw);
    });

    socket.on('chat.closed', ({ chatWith }) => {
        const uid = socket.realUserId;
        const cw = toId(chatWith);
        if (!uid || !cw) return;
        const set = openChats.get(uid);
        if (set) {
            set.delete(cw);
            if (set.size === 0) openChats.delete(uid);
        }
    });

    // ═══════════════════════════════════════════════════════════
    // session.refresh — backward compat (لا شيء)
    // ═══════════════════════════════════════════════════════════
    socket.on('session.refresh', (callback) => {
        if (typeof callback === 'function') callback({ ok: true });
    });

    // ═══════════════════════════════════════════════════════════
    // Manual offline
    // ═══════════════════════════════════════════════════════════
    socket.on('user.offline', () => {
        const uid = socket.realUserId;
        if (!uid) return;
        userTokens.delete(uid);
        const sockets = onlineUsers.get(uid);
        if (!sockets || sockets.size <= 1) {
            markUserOffline(uid, Date.now());
        } else {
            lastActivity.delete(uid);
        }
    });

    // ═══════════════════════════════════════════════════════════
    // Logout
    // ═══════════════════════════════════════════════════════════
    socket.on('member.logout', () => {
        const uid = socket.realUserId;
        if (!uid) return;
        const logoutTime = Date.now();
        const sockets = onlineUsers.get(uid);
        if (sockets) {
            sockets.forEach((sid) => {
                const s = gellybookns.sockets.get(sid);
                if (s) s.disconnect(true);
            });
        }
        onlineUsers.delete(uid);
        userTokens.delete(uid);
        openChats.delete(uid);
        markUserOffline(uid, logoutTime);
        console.log(`🚪 user ${uid} logged out`);
    });

    // ═══════════════════════════════════════════════════════════
    // Disconnect — source of truth for online/offline
    // ═══════════════════════════════════════════════════════════
    socket.on('disconnect', (reason) => {
        const uid = socket.realUserId;
        console.log(`🔌 Disconnected: user=${uid} socket=${socket.id} (${reason})`);

        if (!uid) return;

        const sockets = onlineUsers.get(uid);
        if (sockets) {
            sockets.delete(socket.id);

            if (sockets.size === 0) {
                onlineUsers.delete(uid);
                lastActivity.delete(uid);
                openChats.delete(uid);
                markUserOffline(uid, Date.now());
                broadcastOnlineUsers();
            } else {
                console.log(`ℹ️ user ${uid} still has ${sockets.size} socket(s)`);
            }
        }
    });
});

// ═══════════════════════════════════════════════════════════════
// Start
// ═══════════════════════════════════════════════════════════════
server.listen(port, () => {
    console.log(`\n✅ Socket.IO running on port: ${port}`);
    console.log(`   Path: /gellybook/`);
    console.log(`   pingTimeout: 25s (fast stale detection)`);
    console.log(`   Online/offline: based on disconnect only\n`);
});

// ═══════════════════════════════════════════════════════════════
// Graceful shutdown
// ═══════════════════════════════════════════════════════════════
process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received');
    io.close(() => { server.close(() => process.exit(0)); });
});

process.on('SIGINT', () => {
    console.log('🛑 SIGINT received');
    io.close(() => { server.close(() => process.exit(0)); });
});

process.on('uncaughtException', (err) => {
    console.error('❌ uncaughtException:', err);
    setTimeout(() => process.exit(1), 1000);
});

process.on('unhandledRejection', (reason) => {
    console.error('❌ unhandledRejection:', reason);
});
