const { Kafka } = require("kafkajs");
const axios = require("axios");

const kafka = new Kafka({
  brokers: ["active-phoenix-9525-us1-kafka.upstash.io:9092"],
  sasl: {
    mechanism: "scram-sha-256",
    username: "YWN0aXZlLXBob2VuaXgtOTUyNSQMuoRe8tWWNimpUlMFf65ZMGCZjTTBjPu9UJM",
    password: "Zjg4NmJkZGMtM2ZkNC00MDMxLTljOTQtMjA5OTcxNTRlZDA4",
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
        console.log("Message", parsedMessage);
        console.log("Antes de actualizar", new Date());
        await axios
          .patch(
            `https://alertas-expedientes-api-production.up.railway.app/busqueda?fecha=${parsedMessage.fecha}&exp=${parsedMessage.exp}&extracto=${parsedMessage.extracto}&cve_juz=${parsedMessage.cve_juz}&idExpediente=${parsedMessage.idExpediente}`
          )
          .then((response) => {
            console.log("Response");
          })
          .catch((error) => {
            console.log("Error");
            if (!error.response) {
              // network error
              this.errorStatus = "Error: Network Error";
            } else {
              this.errorStatus = error.response.data.message;
            }
          });

        console.log("Actualizado", new Date());
      },
    });
  } catch (error) {
    console.error("Error en consumeMessages-----------:", error);
  }
}

consumeMessages();
