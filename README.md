# VLESS Cloudflare Worker

A Cloudflare Worker that acts as a VLESS proxy with WebSocket transport, improving connection speed and stability by routing through Cloudflare's CDN network.

## Features

- VLESS protocol over WebSocket
- TLS encryption via Cloudflare
- DNS over HTTPS (UDP DNS queries via Cloudflare DoH)
- Fallback to proxy IP on connection failure
- Auto-generated client config

## Configuration

| Variable | Description |
|----------|-------------|
| `UUID` | Your VLESS UUID for authentication |
| `PROXY_IP` | Your backend server IP |

## Deployment

### Using Wrangler CLI

```bash
npx wrangler deploy
```

### Manual Upload

Upload `worker.js` via Cloudflare Dashboard > Workers & Pages > Create > Upload.

## Usage

### Get VLESS Config

Visit: `https://your-worker-domain/{UUID}`

### Client Config

```
vless://f8673915-f21f-4a52-8d2b-4b1ad1593a71@vless.enzovilo.site:443?encryption=none&security=tls&sni=vless.enzovilo.site&type=ws&host=vless.enzovilo.site&path=%2F%3Fed%3D2048#VLESS-WS-TLS-CF
```

## Client Settings

| Setting | Value |
|---------|-------|
| Address | `vless.enzovilo.site` |
| Port | `443` |
| UUID | `f8673915-f21f-4a52-8d2b-4b1ad1593a71` |
| Encryption | none |
| Transport | WebSocket |
| Security | TLS |
| SNI | `vless.enzovilo.site` |
| Path | `/?ed=2048` |

## Compatible Clients

- v2rayN (Windows)
- v2rayNG (Android)
- Nekoray (Windows/Linux)
- Shadowrocket (iOS)
- Clash Meta / Mihomo

## License

MIT
