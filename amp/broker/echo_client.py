import asyncio


async def tcp_echo_client(message):
    reader, writer = await asyncio.open_connection(
        '127.0.0.1', 3325)

    async def send_message():
        print(f'Send: {message!r}' * 10240)
        writer.write(message.encode())

        data = await reader.read(100)
        print(f'Received: {data.decode()!r}')

    tasks = [send_message() for x in range(10)]
    await asyncio.gather(*tasks)

    print('Close the connection')
    writer.close()

asyncio.run(tcp_echo_client('Hello World!'))
