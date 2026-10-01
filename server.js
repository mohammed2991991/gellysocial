// ═══════════════════════════════════════════════════════════════
// Gellybook Socket.IO Server — نسخة كاملة مُصلَّحة
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
const lastSeen = new Map();        // آخر ظهور حقيقي (disconnect فعلي)
const onlineUsers = new Map();     // userId → Set<socketId>
const lastActivity = new Map();    // داخلي للـ timeout
const userTokens = new Map();
const pendingOffline = new Map();  // userId → timeoutId (Grace period)

const OFFLINE_TIMEOUT = 120000;         // 2 دقيقة idle → offline
const ONLINE_BROADCAST_INTERVAL = 20000; // كل 20 ثانية بث اللي أونلاين
const GRACE_PERIOD = 5000;              // ✅ 5 ثواني قبل إعلان الأوفلاين

let openChats = {};

// ═══════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════
function setOnline(userId, socketId) {
    const id = String(userId);
    if (!onlineUsers.has(id)) {
        onlineUsers.set(id, new Set());
    }
    onlineUsers.get(id).add(socketId);
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

// ✅ تسجيل آخر ظهور
function markUserOffline(userId, timestamp = Date.now()) {
    if (!userId) return;
    const id = String(userId);
    lastSeen.set(id, timestamp);
    lastActivity.delete(id);
    gellybookns.emit('user.offline', { userId: id, lastSeen: timestamp });
    console.log(`💤 user ${id} → offline @ ${new Date(timestamp).toISOString()}`);
}

// ✅ جدولة إعلان الأوفلاين بعد Grace Period
function scheduleOffline(userId) {
    const id = String(userId);

    // لو فيه تايمر قديم — اقفله
    if (pendingOffline.has(id)) {
        clearTimeout(pendingOffline.get(id));
    }

    const timer = setTimeout(() => {
        pendingOffline.delete(id);
        // تأكد إن اليوزر فعلاً مش أونلاين دلوقتي
        if (!onlineUsers.has(id)) {
            markUserOffline(id, Date.now());
            broadcastOnlineUsers();
        }
    }, GRACE_PERIOD);

    pendingOffline.set(id, timer);
    console.log(`⏱️ scheduled offline for user ${id} in ${GRACE_PERIOD/1000}s`);
}

// ✅ إلغاء الأوفلاين المعلّق
function cancelPendingOffline(userId) {
    const id = String(userId);
    if (pendingOffline.has(id)) {
        clearTimeout(pendingOffline.get(id));
        pendingOffline.delete(id);
        console.log(`✋ cancelled pending offline for user ${id}`);
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
// Periodic cleanup — للـ idle timeout
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
            cancelPendingOffline(userId);
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

    // ✅ إلغاء أي أوفلاين معلّق — المستخدم رجع
    if (userId) {
        cancelPendingOffline(userId);

        // ✅ امسح lastSeen بس لو ده أول socket (يعني كان offline فعلاً)
        if (!onlineUsers.has(String(userId))) {
            lastSeen.delete(String(userId));
        }
    }

    if (userId && token) {
        userTokens.set(userId, token);
        if (!lastActivity.has(String(userId)))
            lastActivity.set(String(userId), Date.now());
    }

    // ═══ الانضمام التلقائي + بث فوري ═══
    if (userId) {
        socket.join('chat.' + userId);
        socket.join('user.' + userId);
        setOnline(userId, socket.id);

        // ✅ بث فوري: user.online + friends.online.list
        gellybookns.emit('user.online', {userId: String(userId)});
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

        if (!onlineUsers.has(String(uid))) {
            // المستخدم رجع بعد ما كان offline
            cancelPendingOffline(uid);
            setOnline(uid, socket.id);
            gellybookns.emit('user.online', {userId: String(uid)});
            broadcastOnlineUsers();
        } else {
            onlineUsers.get(String(uid)).add(socket.id);
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
    // get-last-seen
    // ═══════════════════════════════════════════════════════════
    socket.on('get-last-seen', (uid, callback) => {
        const id = String(uid);

        if (onlineUsers.has(id)) {
            return callback({ lastSeen: 'online' });
        }

        const last = lastSeen.get(id);
        return callback({ lastSeen: last || null });
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
    // ✅ user.offline (من beforeunload) — بيأجل مش بيبعت فوراً
    // ═══════════════════════════════════════════════════════════
    socket.on('user.offline', () => {
        const uid = socket.realUserId;
        if (!uid) return;

        console.log(`📤 user.offline received from user=${uid}`);
        userTokens.delete(uid);

        const sockets = onlineUsers.get(String(uid));
        // لو ده آخر socket → جدول أوفلاين (مش فوري)
        if (!sockets || sockets.size <= 1) {
            scheduleOffline(uid);
        } else {
            // فيه sockets تانية → مسح الـ socket ده بس
            sockets.delete(socket.id);
            lastActivity.delete(String(uid));
        }
    });

    // ═══ Logout صريح — إعلان فوري ═══
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
        cancelPendingOffline(uid);       // ✅ إلغاء أي تأجيل
        markUserOffline(uid, logoutTime); // ✅ إعلان فوري
        broadcastOnlineUsers();

        console.log(`🚪 user ${uid} logged out`);
    });

    // ═══════════════════════════════════════════════════════════
    // disconnect
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
                // ✅ جدولة أوفلاين (مش فوري)
                scheduleOffline(uid);
            } else {
                console.log(`ℹ️ user ${uid} still has ${sockets.size} socket(s) → still online`);
            }
        } else {
            scheduleOffline(uid);
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
