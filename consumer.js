const { Kafka } = require("kafkajs");
const axios = require("axios");

const kafka = new Kafka({
  brokers: ["legible-duck-7093-us1-kafka.upstash.io:9092"],
  sasl: {
    mechanism: "scram-sha-256",
    username: "bGVnaWJsZS1kdWNrLTcwOTMk2kYRHkeNGLe30E2DWXiXNOUnLvlZBqDzzR3wO9o",
    password: "MmFiNGJkNDQtYTliMi00ZmQ4LWIxYmUtMjk0ODQxNjM2YjFl",
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
        console.log("Mensaje recibido", parsedMessage);
        console.log("Antes de actualizar", new Date());
        // await axios.patch(
        //   `http://localhost:3000/busqueda?fecha=${parsedMessage.fecha}&exp=${parsedMessage.exp}&extracto=${parsedMessage.extracto}&cve_juz=${parsedMessage.cve_juz}&idExpediente=${parsedMessage.idExpediente}`
        // );

        await axios
          .patch(
            `http://localhost:3000/busqueda?fecha=${parsedMessage.fecha}&exp=${parsedMessage.exp}&extracto=${parsedMessage.extracto}&cve_juz=${parsedMessage.cve_juz}&idExpediente=${parsedMessage.idExpediente}`
          )
          .then((response) => {
            console.log("Response", response);
          })
          .catch((error) => {
            console.log("Error", error);
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
