let session_id = null;
const md = window.markdownit({ breaks: true });
let history = null;
const app_path = "/rag";

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
  let url = `${app_path}/ask/stream?query=${encodeURIComponent(message)}`;
  if (history) {
    url += `&history=${encodeURIComponent(history)}`;
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

  evtSource.addEventListener('history', (event) => {
    setHistory(JSON.parse(event.data));
    console.log("History updated:", history);
  });

  evtSource.addEventListener('rewritten_query', (event) => {
    const data = JSON.parse(event.data);
    console.log("Rewritten query:", data);
    setRewrittenQuery(data);
  });

  evtSource.addEventListener('documents', (event) => {
    const data = JSON.parse(event.data);
    console.log("Documents:", data);
    setDocuments(data);
  });

  function setDocuments(data) {
    const docDiv = document.getElementById('chat-documents');
    const docText = document.getElementById('chat-documents-text');
    docText.innerHTML = ''; // Clear any existing content
    const list = document.createElement('ul');
    data.forEach(doc => {
      const listItem = document.createElement('li');
      listItem.classList.add('document-list-item');
      const link = document.createElement('a');
      link.classList.add('document-link');
      link.href = doc.url;
      link.textContent = doc.url;
      link.target = '_blank'; // Open in a new tab
      const span = document.createElement('span');
      span.classList.add('document-text');
      span.textContent = doc.text;
      listItem.appendChild(link);
      listItem.appendChild(document.createElement('br'));
      listItem.appendChild(span);
      list.appendChild(listItem);
    });
    docText.appendChild(list);
    docDiv.style.display = 'block';
    docDiv.scrollIntoView({ behavior: 'smooth' });
  }

  evtSource.onerror = (err) => {
    console.error("EventSource failed:", err);
    evtSource.close();
  };

});

function setHistory(newHistory) {
  history = newHistory;
  const summaryDiv = document.getElementById('chat-summary');
  const summaryText = document.getElementById('chat-summary-text');
  summaryText.innerHTML = newHistory;
  summaryDiv.style.display = 'block';
  summaryDiv.scrollIntoView({ behavior: 'smooth' });
}

function setRewrittenQuery(query) {
  const rewrittenDiv = document.getElementById('chat-rewritten-query');
  const rewrittenText = document.getElementById('chat-rewritten-query-text');
  rewrittenText.innerHTML = query;
  rewrittenDiv.style.display = 'block';
  rewrittenDiv.scrollIntoView({ behavior: 'smooth' });
}

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
    senderDiv.textContent = 'RAG agent';
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

  const res = await fetch(`${app_path}/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token })
  });

  if (res.ok) {
    setLoggedIn(true);
    loadConfigTable();
    // alert("Logged in!");
  } else {
    alert("Login failed. Please check your token.");
  }
});

async function loadConfigTable() {
  if (document.getElementById('config-container').style.display != 'none') {
    return;
  }

  try {
    const response = await fetch(`${app_path}/config`);
    if (!response.ok) {
      throw new Error('Failed to fetch config');
    }
    const config = await response.json();

    if (config.ready !== true) {
      const existing = document.getElementById('status-pre');
      if (existing) existing.remove();

      const pre = document.createElement('pre');
      pre.id = 'status-pre';
      pre.textContent = config.detail || "Preparing...";
      pre.style.margin = "1em";
      document.body.appendChild(pre);

      // Try again in 3 seconds
      setTimeout(loadConfigTable, 3000);
      return;
  }

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
    setLoggedIn(true);
  } catch (err) {
    console.error('Error loading config:', err, " - might be logged out");
    setLoggedIn(false);
  }
}

document.addEventListener('DOMContentLoaded', () => {
  loadConfigTable();
});

document.getElementById("logout-button").addEventListener("click", () => {
  setLoggedIn(false);
});

async function setLoggedIn(value) {
  console.log("Setting logged in to:", value, " was ", loggedIn);
  if (loggedIn !== value) {
    if (value) {
      console.log("Logging in...");
      document.getElementById('login-popup').style.display = 'none';
      document.getElementById('logout-container').style.display = 'block';
    } else {
      console.log("Logging out...");
      await fetch(`${app_path}/logout`, {
        method: "POST",
        credentials: "include"
      });
      session_token = null;
      location.reload();
    }

    loggedIn = value;
  }
}