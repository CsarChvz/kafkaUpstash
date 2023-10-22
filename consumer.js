const { Kafka } = require('kafkajs');
const axios = require('axios');

interface DatosExpediente {
  fecha: Number;
  exp: Number
  cve_juz: String
  extracto: String
}

const kafka = new Kafka({
    brokers: ['engaging-swan-10732-us1-kafka.upstash.io:9092'],
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'ZW5nYWdpbmctc3dhbi0xMDczMiR4sCuIBVuISYn-VLSLYlosq_1_SK_gQQrC0Uc',
      password: 'ZjBjYjA4ODctZTBiMC00MmQ4LTk3NDItNTQwNWU2Y2QwMzQ4'
    },
    ssl: true,
  })

const consumer = kafka.consumer({ groupId: 'expedientes-busqueda' });

async function consumeMessages() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'expedientes', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const parsedMessage= JSON.parse(message.value.toString())
      console.log(parsedMessage.fecha);
      console.log(
        `Recibido mensaje en ${topic}-${partition} | Offset: ${message.offset}, Fecha: ${parsedMessage.fecha}, Exp: ${parsedMessage.exp}, Juzgado: ${parsedMessage.cve_juz}, Extracto: ${parsedMessage.extracto}`
      );
    },
  });
}

consumeMessages().catch(console.error);
