// Copy buttons and confirmation prompts for the Nexus portal.
document.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-copy]");
  if (!button) return;
  const source = document.getElementById(button.dataset.copy);
  if (!source) return;
  try {
    await navigator.clipboard.writeText(source.textContent.trim());
    const label = button.textContent;
    button.textContent = "Copied";
    setTimeout(() => { button.textContent = label; }, 1500);
  } catch {
    const range = document.createRange();
    range.selectNodeContents(source);
    const selection = window.getSelection();
    selection.removeAllRanges();
    selection.addRange(range);
  }
});

document.addEventListener("submit", (event) => {
  const message = event.target.dataset.confirm;
  if (message && !window.confirm(message)) event.preventDefault();
});
