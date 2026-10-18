// ═══════════════════════════════════════════════════════════════
// Gellybook Socket.IO Server — نسخة كاملة بعد إصلاح آخر ظهور + rooms
// + إصلاح باج number/string + ICE restart + memory leaks
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
    pingInterval: 30000,
    pingTimeout: 90000,
    transports: ['websocket', 'polling'],
});

const gellybookns = io.of('/gellybook');

// ═══════════════════════════════════════════════════════════════
// ✅ Helper: وحّد المفاتيح كـ String دايماً
// (ده أهم إصلاح — كان بيخلي السوكيت يقطع كل دقيقتين)
// ═══════════════════════════════════════════════════════════════
function toId(v) {
    return v == null ? null : String(v);
}

// ═══════════════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════════════
const lastSeen = new Map();        // String userId → timestamp
const onlineUsers = new Map();     // String userId → Set<socketId>
const lastActivity = new Map();    // String userId → timestamp (internal)
const userTokens = new Map();      // String userId → token
const openChats = new Map();       // String userId → Set<chatWith>

const OFFLINE_TIMEOUT = 120000;          // 2 دقايق
const ONLINE_BROADCAST_INTERVAL = 20000; // 20 ثانية

// ═══════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════
function setOnline(userId, socketId) {
    const id = toId(userId);
    if (!id) return;

    if (!onlineUsers.has(id)) {
        onlineUsers.set(id, new Set());
    }
    onlineUsers.get(id).add(socketId);
}

function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', { users: onlineList });
}

function safeEmit(room, event, payload) {
    try {
        gellybookns.to(room).emit(event, payload);
        console.log(`→ emit "${event}" to "${room}"`);
    } catch (e) {
        console.error(`❌ emit failed (${event} → ${room}):`, e.message);
    }
}

/**
 * ✅ تسجيل آخر ظهور للمستخدم — بيستخدم String دايماً
 */
function markUserOffline(userId, timestamp = Date.now()) {
    const id = toId(userId);
    if (!id) return;

    lastSeen.set(id, timestamp);
    lastActivity.delete(id);

    gellybookns.emit('user.offline', { userId: id, lastSeen: timestamp });
    console.log(`💤 user ${id} → offline @ ${new Date(timestamp).toISOString()}`);
}

/**
 * ✅ رومات الإشعارات اللي المستخدم لازم ينضم لها
 */
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
app.get('/', (req, res) => {
    res.send('Socket.IO Server running on port: ' + port);
});

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

    if (!event || !data) {
        return res.status(400).json({ error: 'Missing event or data' });
    }

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
                // ✅ ابعت للـ sender كمان لو موجود (decline/cancel)
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
// Periodic cleanup — timeout + broadcast
// ═══════════════════════════════════════════════════════════════
setInterval(() => {
    const now = Date.now();

    for (const [userId, lastAct] of lastActivity.entries()) {
        const isOnline = onlineUsers.has(userId);
        const idle = now - lastAct;

        if (isOnline && idle > OFFLINE_TIMEOUT) {
            console.log(`⏰ user ${userId} timed out after ${Math.floor(idle / 1000)}s`);

            const sockets = onlineUsers.get(userId);
            if (sockets) {
                sockets.forEach((sid) => {
                    const s = gellybookns.sockets.get(sid);
                    if (s) s.disconnect(true);
                });
            }
            onlineUsers.delete(userId);
            userTokens.delete(userId);
            openChats.delete(userId);
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
    const userId = toId(socket.handshake.auth?.userId); // ✅ String

    socket.userId = userId;
    socket.realUserId = userId;
    socket.token = token;

    console.log(`\n🔌 Connected: user=${userId} socket=${socket.id}`);

    if (userId) {
        // ✅ امسح lastSeen بس لو ده أول socket للمستخدم
        if (!onlineUsers.has(userId)) {
            lastSeen.delete(userId);
        }

        if (token) {
            userTokens.set(userId, token);
        }

        // ✅ سجل النشاط دلوقتي
        lastActivity.set(userId, Date.now());

        // ✅ الانضمام لكل الرومات
        getNotificationRooms(userId).forEach((room) => socket.join(room));

        setOnline(userId, socket.id);
        gellybookns.emit('user.online', { userId });
        broadcastOnlineUsers();

        console.log(`📥 user ${userId} joined rooms:`, getNotificationRooms(userId).join(', '));
    }

    // ═══════════════════════════════════════════════════════════
    // WebRTC — المكالمات
    // ═══════════════════════════════════════════════════════════

    /**
     * ✅ offer — يدعم ICE restart عبر flag isRestart
     */
    socket.on('call.offer', ({ to, offer, type, callerName, isRestart }) => {
        const toId_ = toId(to);
        if (!toId_) return;

        safeEmit('chat.' + toId_, 'call.offer', {
            from: socket.realUserId,
            offer,
            type,
            callerName: callerName || 'مستخدم',
            isRestart: !!isRestart,   // ✅ مرّر الـ flag
        });
    });

    /**
     * ✅ answer — بعد استقبال الـ SDP، بلّغ الطرف التاني بـ call.answered
     *    (كان الكود القديم بيعتمد على call.accept اللي محدش بيبعته)
     */
    socket.on('call.answer', ({ to, answer }) => {
        const toId_ = toId(to);
        if (!toId_) return;

        // مرّر الـ SDP للـ caller
        safeEmit('chat.' + toId_, 'call.answer', {
            from: socket.realUserId,
            answer,
        });

        // ✅ بلّغ الـ caller إن المكالمة اتقبلت (يبدأ الـ timer)
        safeEmit('chat.' + toId_, 'call.answered', {
            from: socket.realUserId,
        });
    });

    socket.on('call.ice', ({ to, candidate }) => {
        const toId_ = toId(to);
        if (!toId_) return;

        safeEmit('chat.' + toId_, 'call.ice', {
            from: socket.realUserId,
            candidate,
        });
    });

    // (احتياطي — لو الفرونت بعت call.accept)
    socket.on('call.accept', ({ toUserId }) => {
        const toId_ = toId(toUserId);
        if (!toId_) return;

        safeEmit('chat.' + toId_, 'call.answered', {
            from: socket.realUserId,
        });
    });

    socket.on('call.reject', ({ toUserId }) => {
        const toId_ = toId(toUserId);
        if (!toId_) return;

        safeEmit('chat.' + toId_, 'call.rejected', {
            from: socket.realUserId,
        });
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
        if (socket.realUserId) {
            lastActivity.set(socket.realUserId, Date.now());
        }
    });

    socket.on('join-profile', (uid) => {
        const id = toId(uid);
        if (id) socket.join('profile.' + id);
    });

    // ═══════════════════════════════════════════════════════════
    // Heartbeat
    // ═══════════════════════════════════════════════════════════
    socket.on('heartbeat', () => {
        const uid = socket.realUserId;
        if (!uid) return;

        lastActivity.set(uid, Date.now());

        if (!onlineUsers.has(uid)) {
            onlineUsers.set(uid, new Set([socket.id]));
            gellybookns.emit('user.online', { userId: uid });
            broadcastOnlineUsers();
        } else {
            onlineUsers.get(uid).add(socket.id);
        }
    });

    socket.on('ping', () => {
        const uid = socket.realUserId;
        if (uid) lastActivity.set(uid, Date.now());
        socket.emit('pong');
    });

    // ═══════════════════════════════════════════════════════════
    // Typing
    // ═══════════════════════════════════════════════════════════
    socket.on('useristyping', ({ sender, receiver }) => {
        if (socket.realUserId) {
            lastActivity.set(socket.realUserId, Date.now());
        }
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

        if (onlineUsers.has(id)) {
            return callback({ lastSeen: 'online' });
        }

        return callback({ lastSeen: lastSeen.get(id) || null });
    });

    // ═══════════════════════════════════════════════════════════
    // Chat opened/closed
    // ═══════════════════════════════════════════════════════════
    socket.on('chat.opened', ({ chatWith }) => {
        const uid = socket.realUserId;
        const cw = toId(chatWith);
        if (!uid || !cw) return;

        if (!openChats.has(uid)) {
            openChats.set(uid, new Set());
        }
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
    // user.offline (manual)
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
    // Disconnect
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
                openChats.delete(uid);       // ✅ امسح الـ memory leak
                markUserOffline(uid, Date.now());
            } else {
                console.log(`ℹ️ user ${uid} still has ${sockets.size} socket(s) → still online`);
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

// ═══════════════════════════════════════════════════════════════
// Graceful shutdown
// ═══════════════════════════════════════════════════════════════
process.on('SIGTERM', () => {
    console.log('🛑 SIGTERM received, closing...');
    io.close(() => {
        server.close(() => process.exit(0));
    });
});

process.on('SIGINT', () => {
    console.log('🛑 SIGINT received, closing...');
    io.close(() => {
        server.close(() => process.exit(0));
    });
});

process.on('uncaughtException', (err) => {
    console.error('❌ uncaughtException:', err);
    // ✅ اخرج — PM2/systemd هيعيد التشغيل
    // حالة العملية مش مضمونة بعد uncaughtException
    setTimeout(() => process.exit(1), 1000);
});

process.on('unhandledRejection', (reason) => {
    console.error('❌ unhandledRejection:', reason);
    // ✅ سجل بس — متخرجش هنا عشان متقتلش السيرفر من promise بسيط
    // (لو الحاجة خطيرة، uncaughtException هتلتقطها)
});
