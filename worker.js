const UUID = 'f8673915-f21f-4a52-8d2b-4b1ad1593a71';
const PROXY_IP = '176.124.221.221';

addEventListener('fetch', (event) => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request) {
  const uuid = UUID;
  const proxyIP = PROXY_IP;
  const url = new URL(request.url);
  const path = url.pathname;

  if (path === '/') {
    return new Response('Worker is running', { status: 200 });
  }

  if (path === '/' + uuid) {
    const vlessConfig = generateConfig(uuid, url.hostname);
    return new Response(vlessConfig, {
      status: 200,
      headers: { 'Content-Type': 'text/plain; charset=utf-8' },
    });
  }

  const upgradeHeader = request.headers.get('Upgrade');
  if (!upgradeHeader || upgradeHeader !== 'websocket') {
    return new Response('Expected WebSocket', { status: 426 });
  }

  const [client, server] = Object.values(new WebSocketPair());
  server.accept();

  const readableStream = makeReadableWebSocketStream(server);
  let remoteSocket = null;
  let udpStreamWrite = null;
  let isDns = false;

  readableStream.pipeTo(new WritableStream({
    async write(chunk) {
      if (remoteSocket) {
        const writer = remoteSocket.writable.getWriter();
        await writer.write(chunk);
        writer.releaseLock();
        return;
      }

      const {
        hasError, addressRemote, portRemote, rawDataIndex, vlessVersion, isUDP
      } = processVlessHeader(chunk, uuid);

      if (hasError) {
        server.close(1000, 'Invalid request');
        return;
      }

      if (isUDP && portRemote === 53) {
        isDns = true;
      }

      const vlessResponseHeader = new Uint8Array([vlessVersion[0], 0]);
      const rawClientData = chunk.slice(rawDataIndex);

      if (isDns) {
        const { write } = await handleUDPOutbound(server, vlessResponseHeader, rawClientData, addressRemote, portRemote);
        udpStreamWrite = write;
        return;
      }

      handleTCPOutbound(server, vlessResponseHeader, rawClientData, addressRemote, portRemote, proxyIP);
    },
  })).catch(() => {
    server.close();
  });

  return new Response(null, { status: 101, webSocket: client });
}

function makeReadableWebSocketStream(ws) {
  let cancelled = false;
  return new ReadableStream({
    start(controller) {
      ws.addEventListener('message', (event) => {
        if (cancelled) return;
        controller.enqueue(event.data);
      });
      ws.addEventListener('close', () => {
        if (cancelled) return;
        controller.close();
      });
      ws.addEventListener('error', (err) => {
        controller.error(err);
      });
    },
    cancel() {
      cancelled = true;
      ws.close();
    },
  });
}

function processVlessHeader(buffer, uuid) {
  const bytes = new Uint8Array(buffer);
  const version = bytes.slice(0, 1);
  const uuidBytes = bytes.slice(1, 17);
  const headerUUID = byteToHex(uuidBytes);

  if (headerUUID !== uuid.replace(/-/g, '')) {
    return { hasError: true };
  }

  const optLength = bytes[17];
  const command = bytes[18 + optLength];

  if (command !== 1 && command !== 2) {
    return { hasError: true };
  }

  const isUDP = command === 2;
  const portRemote = (bytes[18 + optLength + 1] << 8) | bytes[18 + optLength + 2];
  const addressIndex = 18 + optLength + 3;
  const addressType = bytes[addressIndex];
  let addressRemote = '';
  let addressLength = 0;

  switch (addressType) {
    case 1:
      addressRemote = bytes.slice(addressIndex + 1, addressIndex + 5).join('.');
      addressLength = 4;
      break;
    case 2:
      addressLength = bytes[addressIndex + 1];
      addressRemote = new TextDecoder().decode(
        bytes.slice(addressIndex + 2, addressIndex + 2 + addressLength)
      );
      addressLength += 1;
      break;
    case 3:
      addressLength = 16;
      const ipv6 = [];
      for (let i = 0; i < 8; i++) {
        ipv6.push(
          ((bytes[addressIndex + 1 + i * 2]) << 8 | bytes[addressIndex + 1 + i * 2 + 1]).toString(16)
        );
      }
      addressRemote = ipv6.join(':');
      break;
    default:
      return { hasError: true };
  }

  return {
    hasError: false,
    addressRemote,
    portRemote,
    rawDataIndex: addressIndex + 1 + addressLength,
    vlessVersion: version,
    isUDP,
  };
}

async function handleTCPOutbound(ws, vlessResponseHeader, rawClientData, addressRemote, portRemote, proxyIP) {
  let tcpSocket;
  try {
    tcpSocket = connect({ hostname: addressRemote, port: portRemote });
  } catch {
    tcpSocket = connect({ hostname: proxyIP, port: portRemote });
  }

  let headerSent = false;
  const writer = tcpSocket.writable.getWriter();
  await writer.write(rawClientData);
  writer.releaseLock();

  tcpSocket.readable.pipeTo(new WritableStream({
    async write(chunk) {
      if (ws.readyState !== WebSocket.OPEN) {
        throw new Error('WebSocket closed');
      }
      if (!headerSent) {
        const combined = new Uint8Array(vlessResponseHeader.byteLength + chunk.byteLength);
        combined.set(vlessResponseHeader, 0);
        combined.set(new Uint8Array(chunk), vlessResponseHeader.byteLength);
        ws.send(combined.buffer);
        headerSent = true;
      } else {
        ws.send(chunk);
      }
    },
  })).catch(() => {
    try { ws.close(); } catch {}
  });

  tcpSocket.closed.catch(async () => {
    try {
      const fallback = connect({ hostname: proxyIP, port: portRemote });
      const w = fallback.writable.getWriter();
      await w.write(rawClientData);
      w.releaseLock();
      fallback.readable.pipeTo(new WritableStream({
        async write(chunk) {
          if (ws.readyState !== WebSocket.OPEN) return;
          if (!headerSent) {
            const combined = new Uint8Array(vlessResponseHeader.byteLength + chunk.byteLength);
            combined.set(vlessResponseHeader, 0);
            combined.set(new Uint8Array(chunk), vlessResponseHeader.byteLength);
            ws.send(combined.buffer);
            headerSent = true;
          } else {
            ws.send(chunk);
          }
        },
      }));
    } catch {}
  });
}

async function handleUDPOutbound(ws, vlessResponseHeader, rawClientData, addressRemote, portRemote) {
  let headerSent = false;
  const transformStream = new TransformStream({
    transform(chunk, controller) {
      for (let index = 0; index < chunk.byteLength;) {
        const udpPacketLength = new DataView(chunk.buffer, chunk.byteOffset + index, 2).getUint16(0);
        const udpData = chunk.slice(index + 2, index + 2 + udpPacketLength);
        index += 2 + udpPacketLength;
        controller.enqueue(udpData);
      }
    },
  });

  transformStream.readable.pipeTo(new WritableStream({
    async write(chunk) {
      const resp = await fetch('https://1.1.1.1/dns-query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/dns-message' },
        body: chunk,
      });
      const dnsResult = await resp.arrayBuffer();
      const udpSizeBuffer = new Uint8Array([
        (dnsResult.byteLength >> 8) & 0xff,
        dnsResult.byteLength & 0xff,
      ]);

      if (ws.readyState === WebSocket.OPEN) {
        if (!headerSent) {
          const combined = new Uint8Array(
            vlessResponseHeader.byteLength + udpSizeBuffer.byteLength + dnsResult.byteLength
          );
          combined.set(vlessResponseHeader, 0);
          combined.set(udpSizeBuffer, vlessResponseHeader.byteLength);
          combined.set(
            new Uint8Array(dnsResult),
            vlessResponseHeader.byteLength + udpSizeBuffer.byteLength
          );
          ws.send(combined.buffer);
          headerSent = true;
        } else {
          const combined = new Uint8Array(udpSizeBuffer.byteLength + dnsResult.byteLength);
          combined.set(udpSizeBuffer, 0);
          combined.set(new Uint8Array(dnsResult), udpSizeBuffer.byteLength);
          ws.send(combined.buffer);
        }
      }
    },
  }));

  const writer = transformStream.writable.getWriter();
  await writer.write(rawClientData);
  writer.releaseLock();
  return { write: transformStream.writable.getWriter() };
}

function byteToHex(bytes) {
  return Array.from(bytes).map((b) => b.toString(16).padStart(2, '0')).join('');
}

function generateConfig(uuid, host) {
  return 'vless://' + uuid + '@' + host + ':443?encryption=none&security=tls&sni=' + host + '&type=ws&host=' + host + '&path=%2F%3Fed%3D2048#VLESS-WS-TLS-CF';
}
