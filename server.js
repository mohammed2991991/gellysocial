// ═══════════════════════════════════════════════════════════════
// Gellybook Socket.IO Server — نسخة كاملة بعد إصلاح آخر ظهور
// ═══════════════════════════════════════════════════════════════
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import axios from 'axios';
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
app.use(express.json({limit: '1mb'}));
app.use(express.urlencoded({extended: true}));

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
    pingInterval: 30000,
    pingTimeout: 90000,
    transports: ['websocket', 'polling'],
});

const gellybookns = io.of('/gellybook');

// ═══════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════
const lastSeen = new Map();        // ✅ وقت آخر ظهور حقيقي (disconnect فقط)
const onlineUsers = new Map();     // userId → Set<socketId>
const lastActivity = new Map();    // ✅ داخلي بس (للـ timeout)
const userTokens = new Map();
const lastApiUpdate = new Map();

const OFFLINE_TIMEOUT = 120000;
const ONLINE_BROADCAST_INTERVAL = 20000;

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

// ✅ دالة موحّدة لتسجيل آخر ظهور
function markUserOffline(userId, timestamp = Date.now()) {
    if (!userId) return;
    userId = String(userId);
    lastSeen.set(userId, timestamp);
    lastActivity.delete(userId);   // نظّف lastActivity
    gellybookns.emit('user.offline', { userId, lastSeen: timestamp });
    console.log(`💤 user ${userId} → offline @ ${new Date(timestamp).toISOString()}`);
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
// Webhook
// ═══════════════════════════════════════════════════════════════
app.post('/webhook', (req, res) => {
    const {event, data} = req.body;

    console.log(`\n📥 [webhook] event="${event}"`);

    if (!event || !data) {
        return res.status(400).json({error: 'Missing event or data'});
    }

    try {
        switch (event) {
            case 'message.sent':
            case 'message.deleted':
                safeEmit('chat.' + data.sender_id, event, data);
                safeEmit('chat.' + data.receiver_id, event, data);
                break;

            case 'message.sent.group':
            case 'message.deleted.group':
                safeEmit('groups.' + data.data.group_id, event, data.data);
                break;

            case 'message.friendrequestsent':
                safeEmit('friendrequestsent.' + data.receiver_id, event, data);
                break;

            case 'message.friendrequestcanceled':
                safeEmit('friendrequestcanceled.' + data.receiver_id, event, data);
                break;

            case 'post.newpost':
            case 'comment.new':
            case 'comment.reaction':
            case 'story.comment':
            case 'story.reaction':
                safeEmit('newpost.' + data.receiver_id, event, data);
                break;

            case 'follow.page':
                safeEmit('page.' + data.receiver_id, event, data);
                break;

            case 'follow.group':
                safeEmit('group.' + data.receiver_id, event, data);
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
// Periodic cleanup — للـ timeout بس
// ═══════════════════════════════════════════════════════════════
setInterval(() => {
    const now = Date.now();

    for (const [userId, lastAct] of lastActivity.entries()) {
        const isOnline = onlineUsers.has(userId);
        const idle = now - lastAct;

        if (isOnline && idle > OFFLINE_TIMEOUT) {
            const sockets = onlineUsers.get(userId);
            if (sockets) {
                sockets.forEach(sid => {
                    const s = gellybookns.sockets.get(sid);
                    if (s) s.disconnect(true);
                });
            }
            onlineUsers.delete(userId);
            userTokens.delete(userId);
            markUserOffline(userId, now);
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

    socket.userId = userId;
    socket.realUserId = userId;
    socket.token = token;

    console.log(`\n🔌 Connected: user=${userId} socket=${socket.id}`);

    // ✅ امسح lastSeen بس لو ده أول socket للمستخدم
    if (userId && !onlineUsers.has(String(userId))) {
        lastSeen.delete(String(userId));
    }

    if (userId && token) {
        userTokens.set(userId, token);
        if (!lastActivity.has(userId))
            lastActivity.set(userId, Date.now());
    }

    // ═══ الانضمام التلقائي ═══
    if (userId) {
        socket.join('chat.' + userId);
        socket.join('user.' + userId);
        setOnline(String(userId), socket.id);
        gellybookns.emit('user.online', {userId});
        broadcastOnlineUsers();
    }

    // ═══════════════════════════════════════════════════════════
    // WebRTC
    // ═══════════════════════════════════════════════════════════
    socket.on('call.offer', ({ to, offer, type, callerName }) => {
        safeEmit('chat.' + to, 'call.offer', {
            from: socket.realUserId,
            offer, type,
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
        safeEmit('chat.' + to, 'call-ended', {from, duration});
    });

    socket.on('call_missed', ({ to, from, chatId }) => {
        safeEmit('chat.' + to, 'call_missed', {from, chatId});
    });

    // ═══════════════════════════════════════════════════════════
    // join
    // ═══════════════════════════════════════════════════════════
    socket.on('join', (room) => {
        if (typeof room !== 'string' || !room) return;
        socket.join(room);
        if (socket.realUserId) {
            lastActivity.set(String(socket.realUserId), Date.now());
        }
    });

    socket.on('join-profile', (uid) => {
        socket.join(`profile.${uid}`);
    });

    // ═══ Heartbeat ═══
    socket.on('heartbeat', () => {
        const uid = socket.realUserId;
        if (!uid) return;

        lastActivity.set(String(uid), Date.now());

        if (onlineUsers.has(String(uid))) {
            onlineUsers.get(String(uid)).add(socket.id);
        } else {
            onlineUsers.set(String(uid), new Set([socket.id]));
            gellybookns.emit('user.online', {userId: uid});
            broadcastOnlineUsers();
        }
    });

    // ═══ Typing ═══
    socket.on('useristyping', ({ sender, receiver }) => {
        if (socket.realUserId) {
            lastActivity.set(String(socket.realUserId), Date.now());
        }
        safeEmit('chat.' + sender, 'useristyping', {sender, receiver});
        safeEmit('chat.' + receiver, 'useristyping', {sender, receiver});
    });

    // ═══════════════════════════════════════════════════════════
    // ✅ get-last-seen — معدّل
    // ═══════════════════════════════════════════════════════════
    socket.on('get-last-seen', (uid, callback) => {
        const id = String(uid);

        // لو المستخدم online دلوقتي → رجّع online
        if (onlineUsers.has(id)) {
            return callback({ lastSeen: 'online' });
        }

        // ✅ رجّع lastSeen بس — مش lastActivity
        const last = lastSeen.get(id);
        return callback({
            lastSeen: last || null,
        });
    });

    // ═══ Chat opened/closed ═══
    socket.on('chat.opened', ({ chatWith }) => {
        const uid = socket.realUserId;
        if (!uid) return;
        if (!openChats[uid]) openChats[uid] = [];
        if (!openChats[uid].includes(chatWith)) {
            openChats[uid].push(chatWith);
        }
    });

    socket.on('chat.closed', ({ chatWith }) => {
        const uid = socket.realUserId;
        if (!uid || !openChats[uid]) return;
        openChats[uid] = openChats[uid].filter((id) => id !== chatWith);
    });

    // ═══════════════════════════════════════════════════════════
    // ✅ user.offline — معدّل: بيسجّل lastSeen كمان
    // ═══════════════════════════════════════════════════════════
    socket.on('user.offline', () => {
        const uid = socket.realUserId;
        if (!uid) return;

        userTokens.delete(uid);

        // ✅ ما نمسحش lastActivity — نحدّث lastSeen بدل كده
        // (لو لسه فيه sockets تانية للمستخدم، مايتسجّلش آخر ظهور)
        const sockets = onlineUsers.get(String(uid));
        if (!sockets || sockets.size <= 1) {
            markUserOffline(uid, Date.now());
        } else {
            lastActivity.delete(String(uid));
        }
    });

    // ═══ Logout ═══
    socket.on('member.logout', () => {
        const uid = socket.realUserId;
        if (!uid) return;

        const logoutTime = Date.now();
        const sockets = onlineUsers.get(String(uid));

        if (sockets) {
            sockets.forEach((sid) => {
                const s = gellybookns.sockets.get(sid);
                if (s) s.disconnect(true);
            });
        }

        onlineUsers.delete(String(uid));
        markUserOffline(uid, logoutTime);

        console.log(`🚪 user ${uid} logged out`);
    });

    // ═══════════════════════════════════════════════════════════
    // ✅ disconnect — معدّل
    // ═══════════════════════════════════════════════════════════
    socket.on('disconnect', (reason) => {
        const uid = socket.realUserId;
        console.log(`🔌 Disconnected: user=${uid} socket=${socket.id} (${reason})`);

        if (!uid) return;

        const sockets = onlineUsers.get(String(uid));

        if (sockets) {
            sockets.delete(socket.id);

            if (sockets.size === 0) {
                onlineUsers.delete(String(uid));
                markUserOffline(uid, Date.now());
            } else {
                // ✅ لسه فيه sockets تانية — المستخدم لسه online
                console.log(`ℹ️ user ${uid} still has ${sockets.size} socket(s) → still online`);
            }
        } else {
            // ✅ لو مفيش sockets map، برضه سجّل آخر ظهور
            if (!onlineUsers.has(String(uid))) {
                markUserOffline(uid, Date.now());
            }
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
