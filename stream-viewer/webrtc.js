/**
 * Minimal WebRTC client for GStreamer webrtcsink signaling protocol.
 *
 * Connects to the webrtcsink signaling WebSocket, negotiates SDP with
 * a specific producer peer, and returns a MediaStream for playback.
 *
 * Usage:
 *   connectWebRTC(videoEl, 'ws://robot:8443', 'reachymini', {
 *     onConnected: () => { ... },
 *     onDisconnected: () => { ... },
 *     onError: (err) => { ... },
 *   });
 */

function connectWebRTC(videoEl, signalingUrl, peerId, callbacks) {
  let ws = null;
  let pc = null;
  let sessionId = null;
  let closed = false;

  function cleanup() {
    closed = true;
    document.removeEventListener('visibilitychange', onVisibilityChange);
    if (pc) { try { pc.close(); } catch (_) {} pc = null; }
    if (ws) { try { ws.close(); } catch (_) {} ws = null; }
  }

  // When the user switches back to this tab, the browser's video decoder
  // pipeline may have paused. Calling play() restarts it immediately.
  function onVisibilityChange() {
    if (!document.hidden && videoEl && videoEl.srcObject && videoEl.paused) {
      videoEl.play().catch(() => {});
    }
  }
  document.addEventListener('visibilitychange', onVisibilityChange);

  function connect() {
    if (closed) return;

    ws = new WebSocket(signalingUrl);

    ws.onopen = () => {
      // Register as a listener
      ws.send(JSON.stringify({
        type: 'setPeerStatus',
        roles: ['listener'],
        meta: { name: 'stream-viewer' }
      }));
      // List producers to find the one matching our peerId by meta.name
      // (GStreamer webrtcsink uses UUIDs as peer IDs, not the meta name)
      ws.send(JSON.stringify({ type: 'list' }));
    };

    ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);

      if (msg.type === 'list') {
        console.log('Producers:', JSON.stringify(msg.producers));  // add this
        // Find producer by meta.name matching our peerId
        const producer = (msg.producers || []).find(
          p => p.meta && p.meta.name === peerId
        );
        if (producer) {
          console.log('Found producer:', producer.id, 'name:', peerId);
          ws.send(JSON.stringify({
            type: 'startSession',
            peerId: producer.id
          }));
        } else {
          // Fallback: try using peerId directly (older protocol)
          console.warn('Producer not found by name, trying direct peerId:', peerId);
          ws.send(JSON.stringify({
            type: 'startSession',
            peerId: peerId
          }));
        }
      }

      if (msg.type === 'sessionStarted') {
        sessionId = msg.sessionId;
        // Create peer connection
        pc = new RTCPeerConnection({
          iceServers: [{ urls: 'stun:stun.l.google.com:19302' }]
        });

        pc.ontrack = (e) => {
          if (e.streams && e.streams[0]) {
            videoEl.srcObject = e.streams[0];
            // Request minimal playout buffering from the browser (Chrome 123+).
            // The default jitter buffer can grow to 100ms+ under jitter; on LAN/Tailscale
            // jitter is negligible so 0 is safe.
            if (e.receiver && 'jitterBufferTarget' in e.receiver) {
              e.receiver.jitterBufferTarget = 0;
            }
            if (callbacks.onConnected) callbacks.onConnected();
          }
        };

        pc.onicecandidate = (e) => {
          if (e.candidate && ws && ws.readyState === WebSocket.OPEN) {
            ws.send(JSON.stringify({
              type: 'peer',
              sessionId: sessionId,
              ice: {
                candidate: e.candidate.candidate,
                sdpMLineIndex: e.candidate.sdpMLineIndex,
                sdpMid: e.candidate.sdpMid
              }
            }));
          }
        };

        pc.onconnectionstatechange = () => {
          // 'disconnected' is transient — ICE may self-heal. Only treat 'failed' as terminal.
          if (pc.connectionState === 'failed') {
            if (callbacks.onDisconnected) callbacks.onDisconnected();
          }
        };

        // When ICE goes disconnected (e.g. tab was backgrounded and keepalives
        // missed their window), kick a new ICE gathering round immediately
        // rather than waiting up to 30s for the consent-check timeout.
        pc.oniceconnectionstatechange = () => {
          if (pc.iceConnectionState === 'disconnected') {
            setTimeout(() => {
              if (pc && pc.iceConnectionState === 'disconnected') {
                pc.restartIce();
              }
            }, 1500);
          }
        };
      }

      if (msg.type === 'peer' && msg.sessionId === sessionId) {
        if (msg.sdp) {
          // SDP offer from producer
          pc.setRemoteDescription(new RTCSessionDescription(msg.sdp))
            .then(() => pc.createAnswer())
            .then((answer) => {
              pc.setLocalDescription(answer);
              ws.send(JSON.stringify({
                type: 'peer',
                sessionId: sessionId,
                sdp: { type: 'answer', sdp: answer.sdp }
              }));
            })
            .catch((err) => {
              console.error('WebRTC SDP negotiation failed:', err);
              if (callbacks.onError) callbacks.onError(err);
            });
        }
        if (msg.ice) {
          pc.addIceCandidate(new RTCIceCandidate(msg.ice)).catch(() => {});
        }
      }

      if (msg.type === 'error') {
        console.error('Signaling error:', msg);
        if (callbacks.onError) callbacks.onError(new Error(msg.details || 'signaling error'));
      }
    };

    ws.onerror = (err) => {
      console.error('WebSocket error:', err);
      if (callbacks.onError) callbacks.onError(err);
    };

    ws.onclose = () => {
      if (!closed && callbacks.onDisconnected) callbacks.onDisconnected();
    };
  }

  connect();

  // Return a cleanup handle
  return { close: cleanup };
}
