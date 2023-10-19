const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    brokers: ['engaging-swan-10732-us1-kafka.upstash.io:9092'],
    sasl: {
      mechanism: 'scram-sha-256',
      username: 'ZW5nYWdpbmctc3dhbi0xMDczMiR4sCuIBVuISYn-VLSLYlosq_1_SK_gQQrC0Uc',
      password: 'ZjBjYjA4ODctZTBiMC00MmQ4LTk3NDItNTQwNWU2Y2QwMzQ4'
    },
    ssl: true,
  })

const producer = kafka.producer();

async function produceMessage() {
  await producer.connect();
    for (let i = 0; i < 100; i++) {
        await producer.send({
            topic: 'expedientes',
            messages: [
                { value: `Expediente ${i}/2021` },
            ],
        });
    }
  await producer.disconnect();
}

produceMessage().catch(console.error);
