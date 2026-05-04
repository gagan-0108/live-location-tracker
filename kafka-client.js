import { Kafka } from 'kafkajs';

process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';

export const kafkaClient = new Kafka({
  clientId: 'chaicode',
  brokers: process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : ['localhost:9092'],
});
