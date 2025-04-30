let session_id = null;
const md = window.markdownit({ breaks: true });

document.getElementById('chat-button').addEventListener('click', () => {
  if (!loggedIn) {
    showLoginPopup();
    return;
  }
  const popup = document.getElementById('chat-popup');
  popup.style.display = popup.style.display === 'flex' ? 'none' : 'flex';
  popup.style.flexDirection = 'column';
  document.getElementById('chat-input').focus();
});

document.getElementById('chat-input').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault();
    document.getElementById('chat-send').click();
  }
});


document.getElementById('chat-send').addEventListener('click', async () => {
  const input = document.getElementById('chat-input');
  const message = input.value.trim();
  if (!message) return;

  appendMessage("You", message);
  input.value = '';

  // Start the SSE connection for the response
  let url = `/ask/stream?query=${encodeURIComponent(message)}`;
  if (session_id) {
    url += `&session_id=${encodeURIComponent(session_id)}`;
  }

  const evtSource = new EventSource(url);

  let botMessage = '';

  evtSource.addEventListener('start', (event) => {
    const metadata = JSON.parse(event.data);
    session_id = metadata.session_id;
    console.log("Session started:", session_id);
  });

  evtSource.onmessage = (event) => {
    const data = JSON.parse(event.data);
    // console.log("Received message:", data);
    botMessage += data;
    updateBotMessage(botMessage);
  };

  evtSource.addEventListener('end', (event) => {
    // We don't current use anything in the event.data
    console.log("Session ended:", session_id);
    finalizeBotMessage(botMessage);
  });

  evtSource.onerror = (err) => {
    console.error("EventSource failed:", err);
    evtSource.close();
  };

});

function appendMessage(sender, text) {
  const messages = document.getElementById('chat-messages');

  const wrapper = document.createElement('div');
  wrapper.classList.add('chat-message', sender.toLowerCase());

  const senderLabel = document.createElement('span');
  senderLabel.classList.add('chat-sender');
  senderLabel.textContent = sender;
  wrapper.appendChild(senderLabel);

  const body = document.createElement('span');
  body.textContent = text;
  wrapper.appendChild(body);

  messages.appendChild(wrapper);

  messages.scrollTop = messages.scrollHeight;  // Auto-scroll to bottom
}

function updateBotMessage(text) {
  const messages = document.getElementById('chat-messages');

  // Check if the last message is a bot message already
  let last = messages.lastElementChild;

  if (!last || !last.classList.contains('bot')) {
    // Create a fresh bot message wrapper
    last = document.createElement('div');
    last.classList.add('chat-message', 'bot');

    const senderDiv = document.createElement('div');
    senderDiv.classList.add('chat-sender');
    senderDiv.textContent = 'Bot';
    last.appendChild(senderDiv);

    const textDiv = document.createElement('div');
    textDiv.classList.add('chat-text');
    last.appendChild(textDiv);

    messages.appendChild(last);
  }

  // Now update the existing bot message's text
  const textDiv = last.querySelector('.chat-text');
  if (textDiv) {
    textDiv.innerHTML = text.replace(/\n/g, "<br>");
  }

  messages.scrollTop = messages.scrollHeight;
}

function finalizeBotMessage(text) {
  const messages = document.getElementById('chat-messages');
  const last = messages.lastElementChild;

  if (last && last.classList.contains('bot')) {
    const textDiv = last.querySelector('.chat-text');
    if (textDiv) {
      textDiv.innerHTML = md.render(text);
    }
  }

  messages.scrollTop = messages.scrollHeight;
  console.log("Session ended:", session_id);
}

let loggedIn = false;

function showLoginPopup() {
  document.getElementById('login-popup').style.display = 'block';
  document.getElementById('login-token').focus();
}

function hideLoginPopup() {
  document.getElementById('login-popup').style.display = 'none';
}

document.getElementById('login-token').addEventListener('keydown', (e) => {
  if (e.key === 'Enter') {
    e.preventDefault();
    document.getElementById('login-submit').click();
  }
});

document.getElementById('login-submit').addEventListener('click', async () => {
  const token = document.getElementById('login-token').value.trim();
  if (!token) return;

  const res = await fetch("/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token })
  });

  if (res.ok) {
    loggedIn = true;
    hideLoginPopup();
    loadConfigTable();
    // alert("Logged in!");
  } else {
    alert("Login failed. Please check your token.");
  }
});

async function loadConfigTable() {
  try {
    const response = await fetch('/config');
    if (!response.ok) {
      throw new Error('Failed to fetch config');
    }
    const config = await response.json();

    const tbody = document.querySelector('#config-table tbody');
    tbody.innerHTML = '';

    for (const [key, value] of Object.entries(config)) {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td style="border: 1px solid #ccc; padding: 8px;"><b>${key}</b></td>
        <td style="border: 1px solid #ccc; padding: 8px;">${value}</td>
      `;
      tbody.appendChild(row);
    }
    document.getElementById('config-container').style.display = 'table';
  } catch (err) {
    console.error('Error loading config:', err);
  }
}
