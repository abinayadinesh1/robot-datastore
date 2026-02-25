/**
 * WebRTC client using HTTP POST signaling.
 *
 * Browser creates an SDP offer, POSTs it to the server's /offer endpoint,
 * and sets the returned SDP answer. No WebSocket, no STUN/TURN needed
 * on direct networks (LAN / Tailscale).
 *
 * Usage:
 *   connectWebRTC(videoEl, 'http://robot:8000/api/webrtc/offer', {
 *     onConnected: () => { ... },
 *     onDisconnected: () => { ... },
 *     onError: (err) => { ... },
 *   });
 */

function connectWebRTC(videoEl, offerUrl, callbacks) {
  let pc = null;
  let closed = false;

  function cleanup() {
    closed = true;
    document.removeEventListener('visibilitychange', onVisibilityChange);
    if (pc) { try { pc.close(); } catch (_) {} pc = null; }
  }

  function onVisibilityChange() {
    if (!document.hidden && videoEl && videoEl.srcObject && videoEl.paused) {
      videoEl.play().catch(() => {});
    }
  }
  document.addEventListener('visibilitychange', onVisibilityChange);

  async function connect() {
    if (closed) return;

    try {
      pc = new RTCPeerConnection();

      pc.ontrack = (e) => {
        if (e.streams && e.streams[0]) {
          videoEl.srcObject = e.streams[0];
          if (e.receiver && 'jitterBufferTarget' in e.receiver) {
            e.receiver.jitterBufferTarget = 0;
          }
          if (callbacks.onConnected) callbacks.onConnected();
        }
      };

      pc.onconnectionstatechange = () => {
        if (pc.connectionState === 'failed') {
          if (callbacks.onDisconnected) callbacks.onDisconnected();
        }
      };

      pc.addTransceiver('video', { direction: 'recvonly' });

      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);

      // Wait for ICE gathering to finish so all candidates are in the SDP.
      // With HTTP POST signaling there's no trickle ICE channel, so the
      // offer must contain every candidate before we send it.
      if (pc.iceGatheringState !== 'complete') {
        await new Promise((resolve) => {
          pc.addEventListener('icegatheringstatechange', () => {
            if (pc.iceGatheringState === 'complete') resolve();
          });
        });
      }

      const resp = await fetch(offerUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sdp: pc.localDescription.sdp, type: pc.localDescription.type }),
      });

      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

      const answer = await resp.json();
      await pc.setRemoteDescription(answer);
    } catch (err) {
      console.error('WebRTC connect error:', err);
      if (callbacks.onError) callbacks.onError(err);
    }
  }

  connect();

  return { close: cleanup };
}
