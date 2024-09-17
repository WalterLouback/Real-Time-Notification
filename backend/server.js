require('dotenv').config();
const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const mongoose = require('mongoose');
const kafka = require('kafka-node');


const PORT = process.env.PORT || 3000;
const KAFKA_BROKER = process.env.KAFKA_BROKER;
const MONGO_URI = process.env.MONGO_URI;


mongoose.connect(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('MongoDB conectado'))
  .catch(err => console.error('Erro ao conectar ao MongoDB:', err));


const notificationSchema = new mongoose.Schema({
  userId: String,
  message: String,
  date: { type: Date, default: Date.now }
});

const Notification = mongoose.model('Notification', notificationSchema);


const app = express();
const server = http.createServer(app);
const io = socketIO(server);


app.get('/', (req, res) => {
  res.send('Servidor de Notificações em Tempo Real');
});

const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({ kafkaHost: KAFKA_BROKER });
const consumer = new Consumer(
  client,
  [{ topic: 'notifications', partition: 0 }],
  { autoCommit: true }
);

io.on('connection', (socket) => {
  console.log('Novo cliente conectado:', socket.id);

  consumer.on('message', async (message) => {
    const notification = JSON.parse(message.value);

    socket.emit('notification', notification);

    const newNotification = new Notification(notification);
    await newNotification.save();
  });

  socket.on('disconnect', () => {
    console.log('Cliente desconectado:', socket.id);
  });
});
server.listen(PORT, () => {
  console.log(`Servidor rodando na porta ${PORT}`);
});
