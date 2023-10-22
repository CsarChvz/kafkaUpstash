const { Kafka } = require("kafkajs");
const axios = require("axios");

const kafka = new Kafka({
  brokers: ["engaging-swan-10732-us1-kafka.upstash.io:9092"],
  sasl: {
    mechanism: "scram-sha-256",
    username: "ZW5nYWdpbmctc3dhbi0xMDczMiR4sCuIBVuISYn-VLSLYlosq_1_SK_gQQrC0Uc",
    password: "ZjBjYjA4ODctZTBiMC00MmQ4LTk3NDItNTQwNWU2Y2QwMzQ4",
  },
  ssl: true,
});

const consumer = kafka.consumer({ groupId: "expedientes-busqueda" });

async function consumeMessages() {
  try {
    await consumer.connect();
    await consumer.subscribe({ topic: "expedientes", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const parsedMessage = JSON.parse(message.value.toString());

        await axios.post(
          `https://alertas-expedientes-api-production.up.railway.app/busqueda?fecha=${parsedMessage.fecha}&exp=${parsedMessage.exp}&extracto=${parsedMessage.extracto}&cve_juz=${parsedMessage.cve_juz}`
        );

        console.log("Supuestamente actualizado");
      },
    });
  } catch (error) {
    console.error("Error en consumeMessages-----------:", error);
  }
}

consumeMessages();
