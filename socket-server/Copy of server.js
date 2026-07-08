import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import Redis from 'ioredis';

const app = express();
const server = createServer(app);

const io = new Server(server, {
  cors: { origin: '*' ,	methods: ["GET", "POST"]}
});

const redis = new Redis(); // نفس Redis بتاع Laravel

// الاستماع لجميع القنوات
redis.psubscribe('*');
// Node.js Server snippet
redis.subscribe('laravel_database_message.sent'); // أو اسم القناة الافتراضي

/*redis.on('message', function(channel, message) {
    message = JSON.parse(message);
    const data = message.data;
    // إرسال الرسالة للمستقبل فقط
    io.to('chat.' + data.receiver_id).emit('message.sent', data);
    // إرسال الرسالة للمرسل (لتحديث المتصفحات الأخرى المفتوحة لنفس الحساب)
    io.to('chat.' + data.sender_id).emit('message.sent', data);
});*/
/*redis.on('pmessage', (pattern, channel, message) => {
    const payload = JSON.parse(message);

    // غرف المرسل والمستقبل
    const senderRoom = 'chat.' + payload.data.message.member_id;
    const receiverRoom = 'chat.' + payload.data.receiver_id;

    console.log('Sending to sender room:', senderRoom);
    console.log('Sending to receiver room:', receiverRoom);

    // إرسال للمرسل والمستقبل
    io.to(senderRoom).emit(payload.event, payload.data);
    io.to(receiverRoom).emit(payload.event, payload.data);
});*/
/*redis.on('pmessage', (pattern, channel, message) => {
    const payload = JSON.parse(message);

    if (!payload?.data) return;

    const msg = payload.data.message;  // كائن الرسالة
    const receiver_id = payload.data.receiver_id;
    const sender_id = payload.data.sender_id;

    if (!msg || !receiver_id || !sender_id) {
        console.log('Invalid payload:', payload);
        return;
    }

    const senderRoom = 'chat.' + sender_id;
    const receiverRoom = 'chat.' + receiver_id;

    console.log('Sending to sender room:', senderRoom);
    console.log('Sending to receiver room:', receiverRoom);

    io.to(senderRoom).emit(payload.event, payload.data);
    io.to(receiverRoom).emit(payload.event, payload.data);
});*/
redis.on('pmessage', (pattern, channel, message) => {
    const payload = JSON.parse(message);

    if (!payload?.data) return;

    const event = payload.event;
    const data = payload.data;
	if (event === 'message.sent' || event === 'message.delivered' || event === 'message.seen') {
	       const senderRoom = 'chat.' + payload.data.sender_id;
	       const receiverRoom = 'chat.' + payload.data.receiver_id;

	       io.to(senderRoom).emit(event, payload.data);
	       io.to(receiverRoom).emit(event, payload.data);
	   }
    if (event === 'message.sent') {
        const msg = data.message;
        const receiver_id = data.receiver_id;
        const sender_id = data.sender_id;

        if (!msg || !receiver_id || !sender_id) return;

        const senderRoom = 'chat.' + sender_id;
        const receiverRoom = 'chat.' + receiver_id;

        io.to(senderRoom).emit(event, data);
        io.to(receiverRoom).emit(event, data);

    } else if (event === 'message.seen') {
        // هنا نرسل فقط حالة "تمت القراءة" لكل من المرسل والمستقبل
        const msg = data.message;
        const messageId = data.message_id;
        const status = data.status;

        if (!msg || !messageId || !status) return;

        const senderRoom = 'chat.' + msg.member_id; // مرسل الرسالة
        const receiverRoom = 'chat.' + data.receiver_id || msg.member_id; // المستقبل

        io.to(senderRoom).emit(event, data);
        io.to(receiverRoom).emit(event, data);
    }
});

/*redis.on('pmessage', (pattern, channel, message) => {
  const data = JSON.parse(message);

  // إزالة prefix Laravel بدقة
  const cleanChannel = channel.replace(/^.*?-chat\./, 'chat.');

  console.log('Redis:', channel);
  console.log('Clean:', cleanChannel);

  io.to(cleanChannel).emit(data.event, data.data);
});*/


let openChats = {}; // { userId: [chatIds...] }

// التعامل مع الاتصالات
io.on('connection', socket => {
  console.log('New socket connected:', socket.id);

  socket.on('join', room => {
	socket.userId = room;
	const userId=room;
    socket.join(room);
    console.log(`${socket.id} joined room ${room}`);
//	io.to('chat.1').emit('message.sent', {text:"test" });
if (!openChats[userId]) openChats[userId] = [];


  });
  socket.on('chat.opened', ({ chatWith }) => {
      const userId = socket.userId;

      if (!openChats[userId].includes(chatWith)) {
          openChats[userId].push(chatWith);
      }

      // افحص الرسائل اللي لسه مش مقروءة
      chatMessages
          .filter(msg => msg.sender_id === chatWith && msg.receiver_id === userId && msg.status !== 'seen')
          .forEach(msg => {
              msg.status = 'seen';

              // ابعت للمرسل وال المستقبل
              const senderRoom = 'chat.' + msg.sender_id;
              const receiverRoom = 'chat.' + msg.receiver_id;

              io.to(senderRoom).emit('message.seen', { message_id: msg.id, status: 'seen' });
              io.to(receiverRoom).emit('message.seen', { message_id: msg.id, status: 'seen' });
          });
  });


     socket.on('chat.closed', ({ chatWith }) => {
         openChats[socket.userId] = openChats[socket.userId].filter(id => id !== chatWith);
     });
	 // إرسال رسالة
	   socket.on('message.send', (data) => {
	       const { sender_id, receiver_id, content } = data;

	       const message = {
	           id: Date.now(),
	           sender_id,
	           receiver_id,
	           content,
	           status: 'sent',
	       };

	       // تحقق إذا المستقبل فاتح الشات
	       const receiverSockets = Array.from(io.sockets.sockets.values())
	           .filter(s => s.userId === receiver_id);

	       const isOpen = receiverSockets.some(s => openChats[receiver_id]?.includes(sender_id));

	       if (isOpen) message.status = 'seen';

	       // خزّن الرسالة
	       chatMessages.push(message);

	       // أبعت لكل من المرسل والمستقبل
	       receiverSockets.forEach(s => s.emit('message.sent', { message, sender_id, receiver_id }));
	       socket.emit('message.sent', { message, sender_id, receiver_id });

	       // لو المستقبل فاتح، أبعت seen
	       if (message.status === 'seen') {
	           socket.emit('message.seen', { message_id: message.id, status: 'seen' });
	           receiverSockets.forEach(s => s.emit('message.seen', { message_id: message.id, status: 'seen' }));
	       }
	   });
});

server.listen(3000, () => {
  console.log('Socket.IO running on port 3000');
});
