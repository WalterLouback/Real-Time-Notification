const kafka = require('kafka-node');
require('dotenv').config();

const client = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_BROKER || 'localhost:9092' });
const consumer = new kafka.Consumer(
  client,
  [{ topic: 'notifications', partition: 0 }],
  { autoCommit: true }
);

consumer.on('message', (message) => {
  const notification = JSON.parse(message.value);
  console.log('Notificação recebida:', notification);
});

consumer.on('error', (err) => {
  console.error('Erro no consumidor Kafka:', err);
});
