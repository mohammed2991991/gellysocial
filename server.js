import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
// import Redis from 'ioredis'; // علقناها لأننا لم نستخدمها
// import axios from 'axios'; // علقناها مؤقتاً

const app = express();

// routes
app.get("/", (req, res) => {
    res.send("✅ GellySocial Socket Server يعمل بنجاح!");
});

app.get("/health", (req, res) => {
    res.status(200).json({
        status: 'healthy',
        onlineUsers: onlineUsers.size,
        timestamp: new Date().toISOString()
    });
});

const server = createServer(app);
const io = new Server(server, {
    cors: {
        origin: process.env.NODE_ENV === 'production' 
            ? ['https://gellysocial.vercel.app', 'https://yourdomain.com'] // ضع روابط موقعك هنا
            : '*',
        transports: ['websocket', 'polling']
    },
    path: "/gellybook/",
    pingInterval: 5000,
    pingTimeout: 10000
});

// ============================================
// المتغيرات العامة
// ============================================
const lastSeen = new Map();
const onlineUsers = new Map();        // userId -> Set of socket.id
const lastActivity = new Map();       // userId -> last activity timestamp
const userTokens = new Map();         // userId -> token (for API calls)
const lastApiUpdate = new Map();      // userId -> last time we called /api/last-seen

const ONLINE_BROADCAST_INTERVAL = 15000;  // 15 ثانية
const LAST_SEEN_UPDATE_INTERVAL = 60000;  // 60 ثانية

const gellybookns = io.of("/gellybook");

// ============================================
// دوال مساعدة
// ============================================
function setOnline(userId, socketId) {
    if (!userId) return;
    if (!onlineUsers.has(userId)) {
        onlineUsers.set(userId, new Set());
    }
    onlineUsers.get(userId).add(socketId);
}

function updateActivity(userId) {
    if (userId) {
        lastActivity.set(userId, Date.now());
    }
}

function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', { 
        users: onlineList,
        count: onlineList.length,
        timestamp: Date.now()
    });
}

// ============================================
// معالج الأحداث
// ============================================
function handleClientEvents(socket) {
    // 📞 أحداث المكالمات
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

    socket.on('call-ended', (data) => {
        const targetRoom = 'chat.' + data.to;
        gellybookns.to(targetRoom).emit('call-ended', data);
    });

    socket.on('call_missed', ({ to, from, chatId }) => {
        socket.to('chat.' + to).emit('call_missed', {
            from,
            chatId
        });
    });

    // 👤 الانضمام إلى البروفايل
    socket.on('join-profile', userId => {
        if (userId) {
            socket.join(`profile.${userId}`);
        }
    });

    // 💓 نبضات القلب
    socket.on('heartbeat', () => {
        const userId = socket.userId;
        if (!userId) return;

        updateActivity(userId);

        if (onlineUsers.has(userId)) {
            onlineUsers.get(userId).add(socket.id);
        } else {
            onlineUsers.set(userId, new Set([socket.id]));
            gellybookns.emit('user.online', { userId });
            broadcastOnlineUsers();
        }
    });

    // 🚪 الانضمام إلى الغرف
    socket.on('join', (room) => {
        if (!room) return;
        
        socket.join(room);
        const userIdFromRoom = String(room).split('.').pop();

        if (userIdFromRoom) {
            setOnline(userIdFromRoom, socket.id);
            updateActivity(userIdFromRoom);

            if (socket.token) {
                userTokens.set(userIdFromRoom, socket.token);
            }

            socket.userId = userIdFromRoom;
            broadcastOnlineUsers();
        }
    });

    // ⌨️ مؤشر الكتابة
    socket.on("useristyping", ({ sender, receiver }) => {
        if (!sender || !receiver) return;
        
        updateActivity(socket.userId);
        const senderRoom = 'chat.' + sender;
        const receiverRoom = 'chat.' + receiver;
        
        gellybookns.to(senderRoom).emit('useristyping', { sender, receiver });
        gellybookns.to(receiverRoom).emit('useristyping', { sender, receiver });
    });

    // 👁️ الحصول على آخر ظهور
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

    // 📨 أحداث الرسائل
    socket.on('message.sent', (data) => {
        if (!data?.sender_id || !data?.receiver_id) return;
        
        const senderRoom = 'chat.' + data.sender_id;
        const receiverRoom = 'chat.' + data.receiver_id;
        gellybookns.to(senderRoom).emit('message.sent', data);
        gellybookns.to(receiverRoom).emit('message.sent', data);
    });

    socket.on('message.deleted', (data) => {
        if (!data?.sender_id || !data?.receiver_id) return;
        
        const senderRoom = 'chat.' + data.sender_id;
        const receiverRoom = 'chat.' + data.receiver_id;
        gellybookns.to(senderRoom).emit('message.deleted', data);
        gellybookns.to(receiverRoom).emit('message.deleted', data);
    });

    socket.on('message.seen', (data) => {
        const msg = data.message;
        const messageId = data.message_id;
        const status = data.status;
        if (!msg || !messageId || !status) return;
        
        const senderRoom = 'chat.' + msg.member_id;
        const receiverRoom = 'chat.' + (data.receiver_id || msg.member_id);
        gellybookns.to(senderRoom).emit('message.seen', data);
        gellybookns.to(receiverRoom).emit('message.seen', data);
    });

    socket.on('message.delivered', (data) => {
        const msg = data.message;
        const messageId = data.message_id;
        const status = data.status;
        if (!msg || !messageId || !status) return;
        
        const senderRoom = 'chat.' + msg.member_id;
        const receiverRoom = 'chat.' + (data.receiver_id || msg.member_id);
        gellybookns.to(senderRoom).emit('message.delivered', data);
        gellybookns.to(receiverRoom).emit('message.delivered', data);
    });

    // 👥 أحداث المجموعات
    socket.on('message.sent.group', (data) => {
        if (!data?.group_id) return;
        const groupRoom = 'group.' + data.group_id;
        gellybookns.to(groupRoom).emit('message.sent.group', data);
    });

    socket.on('message.deleted.group', (data) => {
        if (!data?.group_id) return;
        const groupRoom = 'group.' + data.group_id;
        gellybookns.to(groupRoom).emit('message.deleted.group', data);
    });

    // 📝 أحداث المنشورات
    socket.on('post.newpost', (data) => {
        if (!data?.receiver_id) return;
        const receiverRoom = 'newpost.' + data.receiver_id;
        gellybookns.to(receiverRoom).emit('post.newpost', data);
    });

    // 👫 أحداث طلبات الصداقة
    socket.on('message.friendrequestsent', (data) => {
        if (!data?.receiver_id) return;
        const receiverRoom = 'friendrequestsent.' + data.receiver_id;
        gellybookns.to(receiverRoom).emit('message.friendrequestsent', data);
    });

    socket.on('message.friendrequestcanceled', (data) => {
        if (!data?.receiver_id) return;
        const receiverRoom = 'friendrequestcanceled.' + data.receiver_id;
        gellybookns.to(receiverRoom).emit('message.friendrequestcanceled', data);
    });

    // 📴 تسجيل الخروج
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

        // قطع جميع اتصالات هذا المستخدم
        const sockets = onlineUsers.get(userId);
        if (sockets) {
            sockets.forEach(sid => {
                const s = gellybookns.sockets.get(sid);
                if (s) s.disconnect(true);
            });
        }

        onlineUsers.delete(userId);
        lastSeen.set(userId, logoutTime);
        lastActivity.set(userId, logoutTime);

        gellybookns.emit('user.offline', {
            userId,
            lastSeen: logoutTime
        });

        broadcastOnlineUsers();
    });

    // 🔌 قطع الاتصال
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

            broadcastOnlineUsers();
        }
    });
}

// ============================================
// اتصال Socket.IO الرئيسي
// ============================================
gellybookns.on('connection', socket => {
    const token = socket.handshake.auth.token;
    const userId = socket.handshake.auth.userId;

    if (userId) {
        lastSeen.delete(userId);
        
        if (token) {
            userTokens.set(userId, token);
        }
        
        if (!lastActivity.has(userId)) {
            lastActivity.set(userId, Date.now());
        }
    }

    socket.userId = userId;
    socket.token = token;

    // معالجة جميع الأحداث
    handleClientEvents(socket);
    
    console.log(`🔌 مستخدم جديد متصل: ${userId || 'غير معروف'} (${socket.id})`);
});

// ============================================
// تحديث lastSeen بشكل دوري
// ============================================
setInterval(async () => {
    const now = Date.now();

    for (const [userId, lastAct] of lastActivity.entries()) {
        const lastUpdate = lastApiUpdate.get(userId) || 0;
        const token = userTokens.get(userId);

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
                // يمكن تفعيل هذا الجزء عند الحاجة
                // await axios.post('http://localhost:8000/api/last-seen', {}, {
                //     headers: { Authorization: 'Bearer ' + token }
                // });
                lastApiUpdate.set(userId, now);
            } catch (err) {
                const status = err.response?.status;
                console.error(`❌ فشل تحديث lastSeen للمستخدم ${userId}: status=${status}`);
                if (status === 401) {
                    userTokens.delete(userId);
                    lastActivity.delete(userId);
                    lastSeen.delete(userId);
                    onlineUsers.delete(userId);
                }
            }
        }
    }

    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

// ============================================
// تشغيل الخادم
// ============================================
const port = process.env.PORT || 8080;
server.listen(port, () => {
    console.log(`🚀 خادم GellySocial Socket.IO يعمل على المنفذ ${port}`);
    console.log(`📊 المسار: /gellybook/`);
    console.log(`🌐 الحالة: http://localhost:${port}/health`);
});

// ============================================
// إيقاف آمن
// ============================================
process.on('SIGTERM', () => {
    console.log('📴 جاري الإيقاف الآمن...');
    server.close(() => {
        console.log('✅ تم إيقاف الخادم');
        process.exit(0);
    });
});

process.on('uncaughtException', (err) => {
    console.error('❌ خطأ غير متوقع:', err);
});

process.on('unhandledRejection', (err) => {
    console.error('❌ خطأ في Promise غير معالج:', err);
});