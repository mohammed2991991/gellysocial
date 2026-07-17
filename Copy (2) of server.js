import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';

const app = express();

// routes
app.get("/", (req, res) => {
    res.send("Work Successfully.");
});

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

// المتغيرات العامة (بدون Redis)
const lastSeen = new Map();
const onlineUsers = new Map();        // userId -> Set of socket.id
const lastActivity = new Map();       // userId -> last activity timestamp
const userTokens = new Map();         // userId -> token (for API calls)
const lastApiUpdate = new Map();      // userId -> last time we called /api/last-seen

const ONLINE_BROADCAST_INTERVAL = 15000;
const LAST_SEEN_UPDATE_INTERVAL = 60000;

const gellybookns = io.of("/gellybook");

// دوال مساعدة
function setOnline(userId, socketId) {
    if (!onlineUsers.has(userId)) {
        onlineUsers.set(userId, new Set());
    }
    onlineUsers.get(userId).add(socketId);
}

function touch(userId) {
    lastActivity.set(userId, Date.now());
}

// دالة بث المستخدمين المتصلين
function broadcastOnlineUsers() {
    const onlineList = Array.from(onlineUsers.keys());
    gellybookns.emit('friends.online.list', { users: onlineList });
}

// دالة لإرسال الأحداث إلى الغرف
function emitToRoom(event, data, room) {
    if (room) {
        gellybookns.to(room).emit(event, data);
    }
}

// دالة لإرسال الأحداث إلى غرف متعددة
function emitToRooms(event, data, rooms) {
    if (Array.isArray(rooms)) {
        rooms.forEach(room => {
            if (room) {
                gellybookns.to(room).emit(event, data);
            }
        });
    } else if (rooms) {
        gellybookns.to(rooms).emit(event, data);
    }
}

// دالة معالجة الأحداث القادمة من العملاء
function handleClientEvents(socket) {
    // أحداث المكالمات
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

    // الانضمام إلى البروفايل
    socket.on('join-profile', userId => {
        socket.join(`profile.${userId}`);
    });

    // نبضات القلب
    socket.on('heartbeat', () => {
        const userId = socket.userId;
        if (!userId) return;

        lastActivity.set(userId, Date.now());

        if (onlineUsers.has(userId)) {
            onlineUsers.get(userId).add(socket.id);
        } else {
            onlineUsers.set(userId, new Set([socket.id]));
            gellybookns.emit('user.online', { userId });
            broadcastOnlineUsers();
        }
        lastActivity.set(userId, Date.now());
    });

    // الانضمام إلى الغرف
    socket.on('join', (room) => {
        socket.join(room);
        const userIdFromRoom = String(room).split('.').pop();

        setOnline(userIdFromRoom, socket.id);

        lastActivity.set(userIdFromRoom, Date.now());

        if (userIdFromRoom && socket.token) {
            userTokens.set(userIdFromRoom, socket.token);
        }

        socket.userId = userIdFromRoom;
        broadcastOnlineUsers();
    });

    // الكتابة
    socket.on("useristyping", ({ sender, receiver }) => {
        lastActivity.set(socket.userId, Date.now());
        const senderRoom = 'chat.' + sender;
        const receiverRoom = 'chat.' + receiver;
        gellybookns.to(senderRoom).emit('useristyping', { sender, receiver });
        gellybookns.to(receiverRoom).emit('useristyping', { sender, receiver });
    });

    // الحصول على آخر ظهور
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

    // فتح وإغلاق الدردشة
    socket.on('chat.opened', ({ chatWith }) => {
        // يمكن إضافة منطق هنا إذا لزم الأمر
    });

    socket.on('chat.closed', ({ chatWith }) => {
        // يمكن إضافة منطق هنا إذا لزم الأمر
    });

    // أحداث الرسائل
    socket.on('message.sent', (data) => {
        const senderRoom = 'chat.' + data.sender_id;
        const receiverRoom = 'chat.' + data.receiver_id;
        gellybookns.to(senderRoom).emit('message.sent', data);
        gellybookns.to(receiverRoom).emit('message.sent', data);
    });

    socket.on('message.deleted', (data) => {
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

    // أحداث المجموعات
    socket.on('message.sent.group', (data) => {
        const groupRoom = 'group.' + data.group_id;
        gellybookns.to(groupRoom).emit('message.sent.group', data);
    });

    socket.on('message.deleted.group', (data) => {
        const groupRoom = 'group.' + data.group_id;
        gellybookns.to(groupRoom).emit('message.deleted.group', data);
    });

    // أحداث المنشورات
    socket.on('post.newpost', (data) => {
        const receiverRoom = 'newpost.' + data.receiver_id;
        gellybookns.to(receiverRoom).emit('post.newpost', data);
    });

    // أحداث طلبات الصداقة
    socket.on('message.friendrequestsent', (data) => {
        const receiverRoom = 'friendrequestsent.' + data.receiver_id;
        gellybookns.to(receiverRoom).emit('message.friendrequestsent', data);
    });

    socket.on('message.friendrequestcanceled', (data) => {
        const receiverRoom = 'friendrequestcanceled.' + data.receiver_id;
        gellybookns.to(receiverRoom).emit('message.friendrequestcanceled', data);
    });

    // تسجيل الخروج
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

    // قطع الاتصال
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

// اتصال Socket.IO الرئيسي
gellybookns.on('connection', socket => {
    const token = socket.handshake.auth.token;
    const userId = socket.handshake.auth.userId;

    lastSeen.delete(userId);

    if (userId && token) {
        userTokens.set(userId, token);
        if (!lastActivity.has(userId)) {
            lastActivity.set(userId, Date.now());
        }
    }

    socket.userId = userId;
    socket.token = token;

    if (userId && token) {
        userTokens.set(userId, token);
    }

    // معالجة جميع الأحداث
    handleClientEvents(socket);
});

// تحديث lastSeen بشكل دوري
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
                // يمكن تفعيل هذا الجزء إذا كنت تريد تحديث lastSeen عبر API
                /* await axios.post('http://localhost:8000/api/last-seen', {}, {
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

    broadcastOnlineUsers();
}, ONLINE_BROADCAST_INTERVAL);

// تشغيل الخادم
server.listen(3000, () => {
    console.log('Socket.IO running on 3000');
});