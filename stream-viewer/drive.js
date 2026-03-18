/**
 * WASD Drive Controller — shared by index.html and robot.html
 *
 * Provides:
 *   getDriveWsUrl(stream)              → ws:// URL string or null
 *   createWasdWidget(options)           → { element, destroy, setConnected, highlightKey }
 *   createDriveController(url, widget, cb) → { connect, disconnect, destroy }
 */

// ── URL derivation ──────────────────────────────────────────────────────────

function getDriveWsUrl(stream) {
  var src = stream.webrtc_url || stream.url || '';
  try {
    return 'ws://' + new URL(src).hostname + ':8420';
  } catch (_) {
    return null;
  }
}

// ── WASD Widget (DOM) ───────────────────────────────────────────────────────

function createWasdWidget(options) {
  // options: { size: 'compact' | 'full', onClose: fn }
  var size = (options && options.size) || 'compact';
  var onClose = options && options.onClose;

  var widget = document.createElement('div');
  widget.className = 'wasd-widget' + (size === 'full' ? ' wasd-full' : '');

  // Key elements keyed by letter
  var keys = {};

  // Row 1: W
  var row1 = document.createElement('div');
  row1.className = 'wasd-row';
  keys.w = makeKey('W');
  row1.appendChild(keys.w);

  // Row 2: A S D
  var row2 = document.createElement('div');
  row2.className = 'wasd-row';
  keys.a = makeKey('A');
  keys.s = makeKey('S');
  keys.d = makeKey('D');
  row2.appendChild(keys.a);
  row2.appendChild(keys.s);
  row2.appendChild(keys.d);

  widget.appendChild(row1);
  widget.appendChild(row2);

  // Connection status
  var status = document.createElement('div');
  status.className = 'drive-status';
  var dot = document.createElement('div');
  dot.className = 'drive-status-dot';
  var label = document.createElement('span');
  label.className = 'drive-status-label';
  label.textContent = 'disconnected';
  status.appendChild(dot);
  status.appendChild(label);
  widget.appendChild(status);

  // Hint
  var hint = document.createElement('div');
  hint.className = 'drive-hint';
  hint.textContent = 'esc to close';
  widget.appendChild(hint);

  function makeKey(letter) {
    var el = document.createElement('div');
    el.className = 'wasd-key';
    el.textContent = letter;
    return el;
  }

  return {
    element: widget,
    destroy: function () {
      if (widget.parentNode) widget.parentNode.removeChild(widget);
    },
    setConnected: function (connected) {
      if (connected) {
        dot.classList.add('connected');
        label.textContent = 'connected';
      } else {
        dot.classList.remove('connected');
        label.textContent = 'disconnected';
      }
    },
    highlightKey: function (key, on) {
      var el = keys[key.toLowerCase()];
      if (el) el.classList.toggle('active', !!on);
    }
  };
}

// ── Drive Controller (WebSocket + key capture) ──────────────────────────────

function createDriveController(wsUrl, widget, callbacks) {
  // callbacks: { onConnect, onDisconnect, onError, onClose }
  var ws = null;
  var active = false;
  var heldKeys = new Set();
  var retryCount = 0;
  var maxRetries = 3;
  var retryTimer = null;

  function send(key, type) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({ key: key, type: type }));
    }
  }

  function onKeyDown(e) {
    if (!active) return;
    var k = e.key.toLowerCase();
    if (k === 'escape') {
      if (callbacks.onClose) callbacks.onClose();
      return;
    }
    if ('wasd'.indexOf(k) === -1) return;
    e.preventDefault();
    e.stopPropagation();
    if (heldKeys.has(k)) return; // ignore OS key-repeat
    heldKeys.add(k);
    widget.highlightKey(k, true);
    send(k, 'down');
  }

  function onKeyUp(e) {
    if (!active) return;
    var k = e.key.toLowerCase();
    if ('wasd'.indexOf(k) === -1) return;
    e.preventDefault();
    e.stopPropagation();
    heldKeys.delete(k);
    widget.highlightKey(k, false);
    send(k, 'up');
  }

  // Release all held keys (e.g. on blur/disconnect)
  function releaseAll() {
    heldKeys.forEach(function (k) {
      widget.highlightKey(k, false);
      send(k, 'up');
    });
    heldKeys.clear();
  }

  function onBlur() {
    releaseAll();
  }

  function connect() {
    active = true;
    document.addEventListener('keydown', onKeyDown, true);
    document.addEventListener('keyup', onKeyUp, true);
    window.addEventListener('blur', onBlur);
    openSocket();
  }

  function openSocket() {
    if (!active) return;
    try {
      ws = new WebSocket(wsUrl);
    } catch (err) {
      if (callbacks.onError) callbacks.onError(err);
      return;
    }

    ws.onopen = function () {
      retryCount = 0;
      if (callbacks.onConnect) callbacks.onConnect();
    };

    ws.onclose = function () {
      releaseAll();
      if (callbacks.onDisconnect) callbacks.onDisconnect();
      if (active && retryCount < maxRetries) {
        retryCount++;
        var delay = Math.min(1000 * Math.pow(2, retryCount - 1), 4000);
        retryTimer = setTimeout(openSocket, delay);
      }
    };

    ws.onerror = function (err) {
      if (callbacks.onError) callbacks.onError(err);
    };
  }

  function disconnect() {
    active = false;
    clearTimeout(retryTimer);
    releaseAll();
    if (ws) {
      try { ws.close(); } catch (_) {}
      ws = null;
    }
  }

  function destroy() {
    disconnect();
    document.removeEventListener('keydown', onKeyDown, true);
    document.removeEventListener('keyup', onKeyUp, true);
    window.removeEventListener('blur', onBlur);
  }

  return {
    connect: connect,
    disconnect: disconnect,
    destroy: destroy
  };
}
