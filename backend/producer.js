// producer.js
const kafka = require('kafka-node');
require('dotenv').config();

const client = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_BROKER || 'localhost:9092' });
const producer = new kafka.Producer(client);

producer.on('ready', () => {
  console.log('Produtor Kafka está pronto');

  // Enviar uma notificação de exemplo
  const notification = {
    userId: 'user123',
    message: 'Você tem uma nova notificação!'
  };

  const payloads = [{ topic: 'notifications', messages: JSON.stringify(notification) }];

  producer.send(payloads, (err, data) => {
    if (err) console.error('Erro ao enviar notificação:', err);
    else console.log('Notificação enviada:', data);
  });
});

producer.on('error', (err) => {
  console.error('Erro no produtor Kafka:', err);
});
