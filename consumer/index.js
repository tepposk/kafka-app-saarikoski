import { Kafka } from 'kafkajs';
console.log("*** Consumer starts... ***");

const kafka = new Kafka({
    clientId: 'checker-server',
    brokers: ['localhost:9092']
});

const consumer = kafka.consumer({ groupId: 'kafka-checker-servers1' });
const producer = kafka.producer();

const validateId = (id) => {
    if (id.length !== 11 || isNaN(id.charAt(0))) {
        return false;
    } else {
        return true;
    }
};

const run = async () => {
    // Consuming
    await consumer.connect();
    await producer.connect();
    await consumer.subscribe({ topic: 'tobechecked', fromBeginning: true });

    await consumer.run({
        eachMessage: async({ topic, partition, message}) => {
            const id = message.value.toString();
            const valid = validateId(id);
            console.log({
                key: message.key.toString(),
                partition: message.partition,
                offset: message.offset,
                value: message.value.toString(),
                valid: valid,
            })
            await producer.send({
                topic: 'checkedresult',
                messages: [
                    {
                        key: message.key.toString(),
                        value: JSON.stringify({ id, valid }),
                    },
                ],
            })
        },
    })
}

run().catch(console.error);